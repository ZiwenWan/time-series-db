/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.index.live.SeriesLoader;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.utils.Time;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Head storage implementation for active time series data.
 * <p>
 * The Head manages recently written time series data before it gets compacted into
 * long-term storage blocks. It provides fast append operations, efficient querying,
 * and coordinates with indexing systems for optimal performance.
 */
public class Head {
    private static final String HEAD_DIR = "head";
    private final HeadAppender.AppendContext appendContext;
    private final long staleChunkExpiry;
    private final Logger log;
    private final LiveSeriesIndex liveSeriesIndex;
    private final SeriesMap seriesMap;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private volatile long maxTime; // volatile to ensure the flush thread sees updates

    /**
     * Constructs a new Head instance.
     *
     * @param dir                     the base directory for head storage
     * @param shardId                 the shard ID for this head
     * @param closedChunkIndexManager the manager for closed chunk indexes
     */
    public Head(Path dir, ShardId shardId, ClosedChunkIndexManager closedChunkIndexManager, Settings indexSettings) {
        log = Loggers.getLogger(Head.class, shardId);
        maxTime = Long.MIN_VALUE;
        seriesMap = new SeriesMap();

        TimeUnit timeUnit = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(indexSettings));
        long chunkRange = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(indexSettings), timeUnit);
        appendContext = new HeadAppender.AppendContext(
            new ChunkOptions(chunkRange, TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.get(indexSettings))
        );
        staleChunkExpiry = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_CHUNK_EXPIRY.get(indexSettings), timeUnit);

        Path headDir = dir.resolve(HEAD_DIR);
        try {
            Files.createDirectories(headDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create the head directory: " + headDir, e);
        }

        try {
            liveSeriesIndex = new LiveSeriesIndex(headDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize the live series index", e);
        }

        this.closedChunkIndexManager = closedChunkIndexManager;

        // rebuild in-memory state
        loadSeries();
    }

    /**
     * Creates a new HeadAppender for appending samples to the head storage.
     *
     * @return a new HeadAppender instance
     */
    public HeadAppender newAppender() {
        return new HeadAppender(this);
    }

    /**
     * Get the SeriesMap for series management.
     *
     * @return the SeriesMap instance
     */
    public SeriesMap getSeriesMap() {
        return seriesMap;
    }

    /**
     * Initialize the min and max time if they are not already set.
     *
     * @param timestamp the timestamp to initialize with
     */
    public void updateMaxSeenTimestamp(long timestamp) {
        if (timestamp > maxTime) {
            maxTime = timestamp;
        }
    }

    /**
     * Get or create a series with the given labels and hash.
     *
     * @param hash      the hash used to get the series
     * @param labels    the labels of the series
     * @param timestamp the timestamp of the first sample in the series, used for indexing
     * @return the series and whether it was newly created
     */
    public SeriesResult getOrCreateSeries(long hash, Labels labels, long timestamp) {
        MemSeries existingSeries = seriesMap.getByReference(hash);
        if (existingSeries != null) {
            return new SeriesResult(existingSeries, false);
        }

        MemSeries newSeries = new MemSeries(hash, labels);
        MemSeries actualSeries = seriesMap.putIfAbsent(newSeries);

        if (actualSeries == newSeries) {
            liveSeriesIndex.addSeries(labels, hash, timestamp);
            return new SeriesResult(newSeries, true);
        } else {
            return new SeriesResult(actualSeries, false);
        }
    }

    /**
     * Get the LiveSeriesIndex for search operations.
     *
     * @return the LiveSeriesIndex instance
     */
    public LiveSeriesIndex getLiveSeriesIndex() {
        return liveSeriesIndex;
    }

    /**
     * Closes all MemChunks in the head that will not have new samples added.
     *
     * @return the minimum sequence number of all in-memory samples after closing chunks
     */
    public long closeHeadChunks() {
        List<MemSeries> allSeries = getSeriesMap().getSeriesMap();
        IndexChunksResult indexChunksResult = indexCloseableChunks(allSeries);

        // translog replays starts from LOCAL_CHECKPOINT_KEY + 1, since it expects the local checkpoint to be the last processed seq no
        // the minSeqNo computed here is the minimum sequence number of all in-memory samples, therefore we must replay it (subtract one)
        long minSeqNoToKeep = indexChunksResult.minSeqNo() - 1;

        closedChunkIndexManager.commitChangedIndexes(allSeries);
        dropClosedChunks(indexChunksResult.seriesToClosedChunks());
        try {
            liveSeriesIndex.commitWithMetadata(allSeries);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }

        // TODO: delegate removal to ReferenceManager
        dropEmptySeries(minSeqNoToKeep);

        // TODO consider returning in an incremental fashion, to avoid no-op reprocessing if the server crashes between CCI commits
        return minSeqNoToKeep;
    }

    /**
     * Indexes all closeable chunks from the given series list.
     *
     * @param seriesList the list of MemSeries to process
     * @return the result containing closed chunks and the minimum sequence number of in-memory samples
     */
    private IndexChunksResult indexCloseableChunks(List<MemSeries> seriesList) {
        long minSeqNo = Long.MAX_VALUE;
        Map<MemSeries, Set<MemChunk>> seriesToClosedChunks = new HashMap<>();
        for (MemSeries series : seriesList) {
            MemSeries.ClosableChunkResult closeableChunkResult = series.getClosableChunks(maxTime, staleChunkExpiry);
            if (closeableChunkResult.minSeqNo() < minSeqNo) {
                minSeqNo = closeableChunkResult.minSeqNo();
            }

            for (MemChunk memChunk : closeableChunkResult.closableChunks()) {
                try {
                    closedChunkIndexManager.addMemChunk(series, memChunk);
                    seriesToClosedChunks.computeIfAbsent(series, k -> new HashSet<>()).add(memChunk);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return new IndexChunksResult(seriesToClosedChunks, minSeqNo);
    }

    /**
     * @param seriesToClosedChunks map of MemSeries to MemChunks to be dropped later
     * @param minSeqNo             minimum sequence number of in-memory/non-flushed samples
     */
    private record IndexChunksResult(Map<MemSeries, Set<MemChunk>> seriesToClosedChunks, long minSeqNo) {
    }

    /**
     * For each key/series in the map, removes the chunks in the corresponding set from the series.
     */
    private void dropClosedChunks(Map<MemSeries, Set<MemChunk>> seriesToClosedChunks) {
        for (Map.Entry<MemSeries, Set<MemChunk>> entry : seriesToClosedChunks.entrySet()) {
            MemSeries series = entry.getKey();
            series.dropClosedChunks(entry.getValue());
        }
    }

    private void dropEmptySeries(long minSeqNoToKeep) {
        List<Long> refs = new ArrayList<>();
        List<MemSeries> allSeries = seriesMap.getSeriesMap();
        for (MemSeries series : allSeries) {
            series.lock();
            try {
                if (series.getMaxSeqNo() >= minSeqNoToKeep) {
                    continue; // cannot gc series that must be loaded for translog replay
                }

                // TODO: Consider proactively removing MemSeries with no chunks. Currently translog replay requires all series that may be
                // appended to be present, and LiveSeriesIndex is used to load them on server start. If we remove them here, it
                // doesn't change the peak memory usage that would be seen after load. However, can reduce the memory footprint
                // during normal operation, and if we load series on demand during translog replay then overall usage can be reduced.
                refs.add(series.getReference());
                seriesMap.delete(series);
            } finally {
                series.unlock();
            }
        }

        try {
            liveSeriesIndex.removeSeries(refs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the current number of series in the head.
     *
     * @return the number of series
     */
    public long getNumSeries() {
        return seriesMap.size();
    }

    private HeadAppender.AppendContext getAppendContext() {
        return appendContext;
    }

    /**
     * Closes the head, flushing any pending writes to disk and writing a snapshot of the head state. Assumes that writes have stopped
     * before this is called.
     *
     * @throws IOException if an error while closing an index occurs
     */
    public void close() throws IOException {
        liveSeriesIndex.close();
        closedChunkIndexManager.close();
    }

    private void loadSeries() {
        liveSeriesIndex.loadSeriesFromIndex(new HeadSeriesLoader());
        log.info("Loaded {} series into head", getNumSeries());

        liveSeriesIndex.updateSeriesFromCommitData(new SeqNoUpdater());
        closedChunkIndexManager.updateSeriesFromCommitData(new MMapTimestampUpdater());
    }

    /**
     * Callback for loading series from the live index into memory.
     */
    private class HeadSeriesLoader implements SeriesLoader {
        @Override
        public void load(MemSeries series) {
            seriesMap.add(series);
            series.markPersisted(); // a series loaded from index is considered persisted
        }
    }

    /**
     * Updates the max sequence number for a series.
     */
    private class SeqNoUpdater implements org.opensearch.tsdb.core.index.live.SeriesUpdater {
        @Override
        public void update(long ref, long seqNo) {
            MemSeries series = seriesMap.getByReference(ref);
            if (series != null) {
                series.setMaxSeqNo(seqNo);
            }
        }
    }

    /**
     * Updates the max MMAPed timestamp for a series.
     */
    private class MMapTimestampUpdater implements org.opensearch.tsdb.core.index.closed.SeriesUpdater {
        @Override
        public void update(long ref, long mmapTimestamp) {
            MemSeries series = seriesMap.getByReference(ref);
            if (series != null) {
                series.setMaxMMapTimestamp(mmapTimestamp);
            }
        }
    }

    /**
     * Result of get or create series operations.
     *
     * @param series  the memory series that was found or created
     * @param created true if a new series was created, false if an existing series was found
     */
    public record SeriesResult(MemSeries series, boolean created) {
    }

    /**
     * Appender implementation for the head storage layer.
     */
    public static class HeadAppender implements Appender {

        private final Head head; // the head storage instance
        private MemSeries series; // the series being appended to
        private Sample sample; // the sample being appended
        private long seqNo; // the sequence number of the sample being appended
        private boolean seriesCreated; // whether the series was created during append

        /**
         * Constructs a HeadAppender for appending a sample to the head.
         *
         * @param head the head storage instance
         */
        public HeadAppender(Head head) {
            this.head = head;
        }

        @Override
        public boolean preprocess(long seqNo, long reference, Labels labels, long timestamp, double value) {
            // TODO: OOO support
            MemSeries series = head.getSeriesMap().getByReference(reference);
            if (series == null) {
                Head.SeriesResult seriesResult = getOrCreateSeries(labels, reference, timestamp);
                series = seriesResult.series();
                seriesCreated = seriesResult.created();
            }
            this.series = series;

            // During translog replay, skip appending in-order samples that are older than the last mmap chunk
            if (timestamp <= series.getMaxMMapTimestamp()) {
                return false;
            }

            head.updateMaxSeenTimestamp(timestamp);
            sample = new FloatSample(timestamp, value);
            this.seqNo = seqNo;
            return seriesCreated;
        }

        private Head.SeriesResult getOrCreateSeries(Labels labels, long hash, long timestamp) {
            if (labels == null || labels.isEmpty()) {
                throw new IllegalStateException("Labels cannot be empty for ref: " + hash + ", timestamp: " + timestamp);
            }

            return head.getOrCreateSeries(hash, labels, timestamp);
        }

        @Override
        public boolean append(Runnable callback) throws InterruptedException {
            return appendSample(head.getAppendContext(), callback);
        }

        /**
         * Appends the pre-processed sample to the resolved series.
         *
         * @param context  the append context containing options for chunk management
         * @param callback optional callback to execute under lock after appending the sample, this persists the series' labels
         * @return true if sample was appended, false otherwise
         * @throws InterruptedException if the thread is interrupted while waiting for the series lock (append failed)
         */
        protected boolean appendSample(AppendContext context, Runnable callback) throws InterruptedException {
            if (series != null) {
                if (!seriesCreated) {
                    // if this thread did not create the series, wait to ensure the series' labels are persisted to the translog
                    series.awaitPersisted();
                }
                series.lock();
                try {
                    callback.run();
                    if (seriesCreated) {
                        // this thread created the series, and the callback has persisted the series' labels, so mark the series as
                        // persisted
                        series.markPersisted();
                    }
                    if (sample == null || series.isOOO(sample.getTimestamp())) {
                        return false; // FIXME: OOO handling - for now skip to ensure compressed chunks are monotonically increasing
                    }

                    series.append(seqNo, sample.getTimestamp(), sample.getValue(), context.options());
                    return true;
                } finally {
                    series.unlock();
                }
            }
            return false;
        }

        /**
         * Context information for appending preprocessed samples.
         *
         * @param options configuration options for chunk management
         */
        public record AppendContext(ChunkOptions options) {
        }
    }

    /**
     * Returns a chunk reader for accessing in-memory chunks from the head storage.
     *
     * @return a HeadChunkReader
     */
    public MemChunkReader getChunkReader() {
        return new HeadChunkReader();
    }

    private class HeadChunkReader implements MemChunkReader {

        @Override
        public List<Chunk> getChunks(long reference) {
            MemSeries series = seriesMap.getByReference(reference);

            if (series == null) {
                return List.of();
            }

            List<Chunk> chunks = new ArrayList<>();
            series.lock();
            try {
                MemChunk current = series.getHeadChunk();
                while (current != null) {
                    chunks.add(current.getChunk());
                    current = current.getPrev();
                }
            } finally {
                series.unlock();
            }

            return chunks;
        }
    }
}
