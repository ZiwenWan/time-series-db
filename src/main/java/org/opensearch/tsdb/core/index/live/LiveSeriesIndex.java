/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
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
 * LiveChunkIndex indexes series in the head block which have open chunks.
 */
public class LiveSeriesIndex {
    /**
     * Directory name for the live series index
     */
    protected static final String INDEX_DIR_NAME = "live_series_index";
    private static final String SERIES_METADATA_KEY = "live_series_metadata";
    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final ReaderManager directoryReaderManager;

    /**
     * Creates a new LiveSeriesIndex in the given directory.
     * @param dir parent dir for the index
     * @throws IOException if opening the index fails
     */
    public LiveSeriesIndex(Path dir) throws IOException {
        Path indexPath = dir.resolve(INDEX_DIR_NAME);
        if (Files.notExists(indexPath)) {
            Files.createDirectory(indexPath);
        }

        analyzer = new WhitespaceAnalyzer();
        directory = new MMapDirectory(indexPath);
        try {
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Use SnapshotDeletionPolicy to allow taking snapshots during recovery
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            this.snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            indexWriter = new IndexWriter(directory, iwc);
            directoryReaderManager = new ReaderManager(DirectoryReader.open(indexWriter, true, false));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LiveSeriesIndex at: " + dir, e);
        }

    }

    /**
     * Add a new series
     * @param labels series labels
     * @param reference series ref
     * @param minTimestamp series creation time
     */
    public void addSeries(Labels labels, long reference, long minTimestamp) {
        Document doc = new Document();
        for (BytesRef labelRef : labels.toKeyValueBytesRefs()) {
            doc.add(new StringField(Constants.IndexSchema.LABELS, labelRef, Field.Store.NO));
            doc.add(new SortedSetDocValuesField(Constants.IndexSchema.LABELS, labelRef));
        }
        doc.add(new LongPoint(Constants.IndexSchema.REFERENCE, reference));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, reference));
        doc.add(new LongPoint(Constants.IndexSchema.MIN_TIMESTAMP, minTimestamp));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, minTimestamp));
        doc.add(new LongPoint(Constants.IndexSchema.MAX_TIMESTAMP, Long.MAX_VALUE)); // live chunks assumed to max infinite max timestamp
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MAX_TIMESTAMP, Long.MAX_VALUE));
        try {
            indexWriter.addDocument(doc);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Remove series by reference
     * @param references series references to remove series for
     * @throws IOException if removing fails
     */
    public void removeSeries(List<Long> references) throws IOException {
        Query query = LongPoint.newSetQuery(Constants.IndexSchema.REFERENCE, references);
        indexWriter.deleteDocuments(query);
    }

    /**
     * Creates MemSeries in the given head based on references/labels stored in the index, as well as metadata in the LiveCommitData
     *
     * @param callback callback to load series into
     * @return the max reference seen
     */
    public long loadSeriesFromIndex(SeriesLoader callback) {
        DirectoryReader reader = null;
        try {
            reader = directoryReaderManager.acquire();
            IndexSearcher searcher = new IndexSearcher(reader);
            return searcher.search(new MatchAllDocsQuery(), new SeriesLoadingCollectorManager(callback));
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            if (reader != null) {
                try {
                    directoryReaderManager.release(reader);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to release searcher", e);
                }
            }
        }
    }

    /**
     * Close the index
     * @throws IOException if closing fails
     */
    public void close() throws IOException {
        indexWriter.close();
        directory.close();
        directoryReaderManager.close();
        analyzer.close();
    }

    /**
     * Commit the index
     * @throws IOException if commit fails
     */
    public void commit() throws IOException {
        indexWriter.commit();
    }

    /**
     * Get the ReaderManager for this index
     * @return ReaderManager for this index
     */
    public ReaderManager getDirectoryReaderManager() {
        return directoryReaderManager;
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

    /**
     * Commit the current state, including live series references and their max sequence numbers. MaxSeqNo is used to remove stale series,
     * i.e. series that have not received any writes since the checkpoint.
     *
     * @param liveSeries the list of live series to include in the commit metadata
     */
    public void commitWithMetadata(List<MemSeries> liveSeries) {
        Map<String, String> commitData = new HashMap<>();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVLong(liveSeries.size());
            for (MemSeries series : liveSeries) {
                output.writeVLong(series.getReference());
                output.writeVLong(series.getMaxSeqNo());
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
     * Update MemSeries with the correct maxSeqNo values from the commit data.
     *
     * @param seriesUpdater the SeriesUpdater to use for updating series
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
                                long seqNo = input.readVLong();
                                seriesUpdater.update(ref, seqNo);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private void commitWithMetadata(Iterable<Map.Entry<String, String>> commitData) throws IOException {
        indexWriter.setLiveCommitData(commitData, true); // force increment version
        indexWriter.commit();
    }
}
