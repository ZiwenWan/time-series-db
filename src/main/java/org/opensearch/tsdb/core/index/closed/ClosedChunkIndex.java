/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple head index that stores chunks, current as one doc per chunk.
 */
public class ClosedChunkIndex {
    private static final String SERIES_METADATA_KEY = "live_series_metadata";
    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final ReaderManager directoryReaderManager;

    /**
     * Create a new ClosedChunkIndex in the given directory.
     * @param dir the directory to store the index
     * @throws IOException if there is an error creating the index
     */
    public ClosedChunkIndex(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            Files.createDirectory(dir);
        }

        analyzer = new WhitespaceAnalyzer();
        directory = new MMapDirectory(dir);
        try {
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Use SnapshotDeletionPolicy to allow taking snapshots during recovery
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            this.snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            SortField primarySortField = new SortField(Constants.IndexSchema.LABELS_HASH, SortField.Type.LONG, false); // ascending
            SortField secondarySortField = new SortField(Constants.IndexSchema.MIN_TIMESTAMP, SortField.Type.LONG, false); // ascending
            Sort indexSort = new Sort(primarySortField, secondarySortField);
            iwc.setIndexSort(indexSort);

            indexWriter = new IndexWriter(directory, iwc);
            directoryReaderManager = new ReaderManager(DirectoryReader.open(indexWriter));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize ClosedChunkIndex at: " + dir, e);
        }
    }

    /**
     * Add a new MemChunk to the index.
     * @param labels the Labels for the series
     * @param memChunk the MemChunk to add
     * @throws IOException if there is an error adding the chunk
     */
    public void addNewChunk(Labels labels, MemChunk memChunk) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.LABELS_HASH, labels.stableHash()));
        for (BytesRef labelRef : labels.toKeyValueBytesRefs()) {
            doc.add(new StringField(Constants.IndexSchema.LABELS, labelRef, Field.Store.NO));
            doc.add(new SortedSetDocValuesField(Constants.IndexSchema.LABELS, labelRef));
        }
        doc.add(new BinaryDocValuesField(Constants.IndexSchema.CHUNK, ClosedChunkIndexIO.serializeChunk(memChunk.getChunk())));
        doc.add(new LongPoint(Constants.IndexSchema.MIN_TIMESTAMP, memChunk.getMinTimestamp()));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, memChunk.getMinTimestamp()));
        doc.add(new LongPoint(Constants.IndexSchema.MAX_TIMESTAMP, memChunk.getMaxTimestamp()));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MAX_TIMESTAMP, memChunk.getMaxTimestamp()));
        indexWriter.addDocument(doc);
    }

    /**
     * Force a merge of the index segments. This is an expensive operation and should be used sparingly.
     */
    public void forceMerge() {
        try {
            indexWriter.forceMerge(1);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Close the index and release all resources.
     */
    public void close() {
        try {
            analyzer.close();
            indexWriter.close();
            directory.close();
            directoryReaderManager.close();
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Get the ReaderManager where the index is stored.
     * @return the ReaderManager
     */
    public ReaderManager getDirectoryReaderManager() {
        return directoryReaderManager;
    }

    /**
     * Commit the current state, including live series references and their max mmap timestamps. This data is used during translog replay to
     * skip adding samples for data that has already been committed.
     *
     * @param liveSeries the list of live series to include in the commit metadata
     */
    public void commitWithMetadata(List<MemSeries> liveSeries) {
        Map<String, String> commitData = new HashMap<>();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVLong(liveSeries.size());
            for (MemSeries series : liveSeries) {
                output.writeVLong(series.getReference());
                output.writeVLong(series.getMaxMMapTimestamp());
            }
            String liveSeriesMetadata = new String(Base64.getEncoder().encode(output.bytes().toBytesRef().bytes), StandardCharsets.UTF_8);
            commitData.put(SERIES_METADATA_KEY, liveSeriesMetadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize live series", e);
        }

        try {
            commitWithMetadata(() -> commitData.entrySet().iterator());
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Update the series with the correct metadata values from the commit data.
     *
     * @param seriesUpdater seriesUpdater used to update the series
     */
    public void updateSeriesFromCommitData(SeriesUpdater seriesUpdater) {
        Iterable<Map.Entry<String, String>> commitData = indexWriter.getLiveCommitData();
        if (commitData == null) {
            return;
        }

        try {
            for (Map.Entry<String, String> entry : commitData) {
                if (entry.getKey().equals(SERIES_METADATA_KEY)) {
                    String seriesMetadata = entry.getValue();
                    byte[] bytes = Base64.getDecoder().decode(seriesMetadata);
                    try (BytesStreamInput input = new BytesStreamInput(bytes)) {
                        if (input.available() > 0) {
                            long numSeries = input.readVLong();
                            for (int i = 0; i < numSeries; i++) {
                                long ref = input.readVLong();
                                long ts = input.readVLong();
                                seriesUpdater.update(ref, ts);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private void commitWithMetadata(Iterable<Map.Entry<String, String>> commitData) throws IOException {
        indexWriter.setLiveCommitData(commitData, true);
        indexWriter.commit();
    }

    /**
     * Take a snapshot of the current commit to protect it from deletion during recovery.
     *
     * @return IndexCommit snapshot that is protected from deletion
     * @throws IOException if snapshot fails
     */
    public IndexCommit snapshot() throws IOException {
        return snapshotDeletionPolicy.snapshot();
    }

    /**
     * Release a previously taken snapshot, allowing cleanup of associated files.
     *
     * @param snapshot the snapshot to release
     * @throws IOException if release fails
     */
    public void release(IndexCommit snapshot) throws IOException {
        snapshotDeletionPolicy.release(snapshot);
        indexWriter.deleteUnusedFiles();
    }
}
