/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.index.ReaderManagerWithMetadata;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.MemChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Reference manager for TSDBDirectoryReader that handles initiation and refreshing of TSDBDirectoryReader instances.
 * It manages acquiring and releasing of underlying DirectoryReaders from live series index and closed chunk indexes.
 * Life Cycle:
 * TSDBDirectoryReaderReferenceManager is created once per TSDBEngine instance and lives as long as the TSDBEngine.
 * **/
@SuppressForbidden(reason = "Reference managing is required here")
public class TSDBDirectoryReaderReferenceManager extends ReferenceManager<OpenSearchDirectoryReader> {

    private static final Logger log = LogManager.getLogger(TSDBDirectoryReaderReferenceManager.class);
    private final ReaderManager liveSeriesIndexReaderManager;
    private final LongSupplier minLiveTimestampSupplier;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private final MemChunkReader memChunkReader;
    private final LabelStorageType labelStorageType;
    private final ShardId shardId;

    private volatile List<ReaderManagerWithMetadata> closedChunkIndexReaderManagers;

    /**
     * Creates a new TSDBDirectoryReaderReferenceManager.
     * @param liveSeriesIndexReaderManager the reader manager for live series index
     * @param closedChunkIndexManager the manager for closed chunk indices
     * @param memChunkReader the reader for memory chunks
     * @param labelStorageType the storage type configured for labels
     * @param shardId the shard identifier
     * @throws IOException if an I/O error occurs during initialization
     */
    // TODO : Pass in data structure to hold already mmaped chunks
    public TSDBDirectoryReaderReferenceManager(
        ReaderManager liveSeriesIndexReaderManager,
        LongSupplier minLiveTimestampSupplier,
        ClosedChunkIndexManager closedChunkIndexManager,
        MemChunkReader memChunkReader,
        LabelStorageType labelStorageType,
        ShardId shardId
    ) throws IOException {

        this.liveSeriesIndexReaderManager = liveSeriesIndexReaderManager;
        this.minLiveTimestampSupplier = minLiveTimestampSupplier;
        this.closedChunkIndexManager = closedChunkIndexManager;
        this.closedChunkIndexReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();
        this.memChunkReader = memChunkReader;
        this.labelStorageType = labelStorageType;
        this.shardId = shardId;

        // initiate the MDR here
        this.current = OpenSearchDirectoryReader.wrap(
            creatNewTSDBDirectoryReader(
                liveSeriesIndexReaderManager,
                minLiveTimestampSupplier,
                closedChunkIndexManager,
                memChunkReader,
                labelStorageType,
                0L
            ),
            shardId
        );
    }

    private TSDBDirectoryReader creatNewTSDBDirectoryReader(
        ReaderManager liveSeriesIndexReaderManager,
        LongSupplier minLiveTimestampSupplier,
        ClosedChunkIndexManager closedChunkIndexManager,
        MemChunkReader memchunkReader,
        LabelStorageType labelStorageType,
        long currentVersion
    ) throws IOException {
        // Collect all closed chunk index reader managers
        // List<ReaderManager> allClosedReaderManagers = closedChunkIndexManager.getReaderManagers();
        List<ReaderManagerWithMetadata> allClosedReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();

        // Acquire DirectoryReader instances from all ReaderManagers
        DirectoryReader liveReader = null;
        List<DirectoryReaderWithMetadata> closedReaders = new ArrayList<>();

        try {
            liveSeriesIndexReaderManager.maybeRefreshBlocking();
            liveReader = liveSeriesIndexReaderManager.acquire();

            for (ReaderManagerWithMetadata readerManagerWithMetadata : allClosedReaderManagers) {
                ReaderManager readerManager = readerManagerWithMetadata.readerMananger();
                readerManager.maybeRefreshBlocking();
                closedReaders.add(
                    new DirectoryReaderWithMetadata(
                        readerManager.acquire(),
                        readerManagerWithMetadata.minTimestamp(),
                        readerManagerWithMetadata.maxTimestamp()
                    )
                );
            }

            log.info("Refreshing closed reader managers, total readers: {}", closedReaders.size());
            return new TSDBDirectoryReader(
                liveReader,
                minLiveTimestampSupplier,
                closedReaders,
                memchunkReader,
                labelStorageType,
                currentVersion + 1
            );

        } catch (IOException | AlreadyClosedException e) {
            log.error("Error creating TSDBDirectoryReader: ", e);
            throw e;
        } finally {
            if (liveReader != null) {
                liveSeriesIndexReaderManager.release(liveReader);
            }
            for (int i = 0; i < closedReaders.size(); i++) {
                if (i < allClosedReaderManagers.size()) {
                    allClosedReaderManagers.get(i).readerMananger().release(closedReaders.get(i).reader());
                }
            }
        }
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        // Only calling decRef on OpenSearchDirectoryReader which will call decRef on underlying TSDBDirectoryReader
        // This does not decRef the underlying DirectoryReaders as they are managed by their respective ReaderManagers
        reference.decRef();
    }

    /**
     * Refreshes the reader if needed. Performs either a structural refresh (if indexes were added/removed)
     * or a lightweight refresh (if only data within existing indexes changed).
     *
     * @param referenceToRefresh the current reader reference
     * @return a new reader if refresh occurred, or null if no refresh was needed
     * @throws IOException if an I/O error occurs during refresh
     */
    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        List<ReaderManagerWithMetadata> currentReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();

        if (!this.closedChunkIndexReaderManagers.equals(currentReaderManagers)) {
            // Structural change detected - indexes were added or removed
            final OpenSearchDirectoryReader reader = OpenSearchDirectoryReader.wrap(
                creatNewTSDBDirectoryReader(
                    liveSeriesIndexReaderManager,
                    minLiveTimestampSupplier,
                    closedChunkIndexManager,
                    memChunkReader,
                    labelStorageType,
                    this.current.getVersion()
                ),
                shardId
            );

            // Update snapshot to prevent redundant structural refreshes
            this.closedChunkIndexReaderManagers = currentReaderManagers;

            log.info("Refreshed the tsdb directory reader");
            return reader;
        } else {
            log.info("No changes detected for refreshing the tsdb directory reader");
            // No structural change - attempt lightweight refresh
            return (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        }
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}
