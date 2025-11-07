/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.engine.TSDBTragicException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LiveSeriesIndexTests extends OpenSearchTestCase {

    public void testAddAndRead() throws IOException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(createTempDir("testAddAndRead"));
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k2", "v2"), 0L, 100L);
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k3", "v3"), 10L, 100L);
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k4", "v4"), 20L, 200L);

        List<Long> refs = getReferences(liveSeriesIndex, buildQuery("/.*/", 0, Long.MAX_VALUE));
        assertTrue("There should be no results before refresh", refs.isEmpty());

        liveSeriesIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        refs = getReferences(liveSeriesIndex, buildQuery("/k1:v1/", 50, Long.MAX_VALUE));
        assertEquals(List.of(0L, 10L, 20L), refs);

        refs = getReferences(liveSeriesIndex, buildQuery("/k1:v1/", 120, 150));
        assertEquals(List.of(0L, 10L), refs);

        refs = getReferences(liveSeriesIndex, buildQuery("/k1:v.*/", 50, Long.MAX_VALUE));
        assertEquals(List.of(0L, 10L, 20L), refs);

        refs = getReferences(liveSeriesIndex, buildQuery("/k4:.*/", 50, Long.MAX_VALUE));
        assertEquals(List.of(20L), refs);

        // Validate series deletion
        liveSeriesIndex.removeSeries(List.of(0L, 10L));
        liveSeriesIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        refs = getReferences(liveSeriesIndex, buildQuery("/k1:v1/", 50, Long.MAX_VALUE));
        assertEquals(List.of(20L), refs);

        refs = getReferences(liveSeriesIndex, buildQuery("/k2:v2/", 50, Long.MAX_VALUE));
        assertEquals(List.of(), refs);

        liveSeriesIndex.close();
    }

    public void testLoadSeries() throws IOException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(createTempDir("testLoadSeries"));
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        Labels labels3 = ByteLabels.fromStrings("k1", "v1", "k4", "v4");

        liveSeriesIndex.addSeries(labels1, 0L, 40L);
        liveSeriesIndex.addSeries(labels2, 10L, 50L);
        liveSeriesIndex.addSeries(labels3, 20L, 60L);

        liveSeriesIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        // mockSeriesLoader checks that load() is called once for each series
        SeriesLoader mockSeriesLoader = new SeriesLoader() {
            private final Set<Long> processedReferences = new HashSet<>();

            @Override
            public void load(MemSeries series) {
                long ref = series.getReference();
                if (!processedReferences.add(ref)) {
                    fail("Duplicate series reference: " + ref);
                }

                if (ref == 0L) {
                    assertEquals(labels1, series.getLabels());
                } else if (ref == 10L) {
                    assertEquals(labels2, series.getLabels());
                } else if (ref == 20L) {
                    assertEquals(labels3, series.getLabels());
                } else {
                    fail("Unexpected series reference: " + ref);
                }
            }
        };

        long maxRef = liveSeriesIndex.loadSeriesFromIndex(mockSeriesLoader);
        assertEquals(20L, maxRef);
        liveSeriesIndex.close();
    }

    public void testCommit() throws IOException {
        Path tempDir = createTempDir("testCommit");
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(tempDir);
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k2", "v2"), 0L, 100L);
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k3", "v3"), 10L, 200L);
        liveSeriesIndex.commit();
        liveSeriesIndex.close();

        LiveSeriesIndex reopenedIndex = new LiveSeriesIndex(tempDir);

        List<Long> refs = getReferences(reopenedIndex, buildQuery("/.*/", 0, Long.MAX_VALUE));
        assertEquals("Should find both persisted series", 2, refs.size());
        assertTrue("Should contain reference 0", refs.contains(0L));
        assertTrue("Should contain reference 10", refs.contains(10L));

        reopenedIndex.close();
    }

    public void testCommitWithMetadataAndLoad() throws IOException {
        Path tempDir = createTempDir("testCommitWithMetadataAndLoad");
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(tempDir);
        MemSeries series1 = new MemSeries(1L, ByteLabels.fromStrings("k1", "v1", "k2", "v2"));
        MemSeries series2 = new MemSeries(2L, ByteLabels.fromStrings("k1", "v1", "k2", "v3"));
        series1.setMaxSeqNo(100);
        series2.setMaxSeqNo(999);

        liveSeriesIndex.addSeries(series1.getLabels(), series1.getReference(), 100L);
        liveSeriesIndex.addSeries(series2.getLabels(), series2.getReference(), 200L);
        liveSeriesIndex.commitWithMetadata(List.of(series1, series2));
        liveSeriesIndex.close();

        LiveSeriesIndex reopenedIndex = new LiveSeriesIndex(tempDir);
        List<Long> refs = getReferences(reopenedIndex, buildQuery("/.*/", 0, Long.MAX_VALUE)); // ensure reader is refreshed
        assertEquals("Should find both persisted series", 2, refs.size());
        assertTrue("Should contain reference 1", refs.contains(1L));
        assertTrue("Should contain reference 2", refs.contains(2L));

        Map<Long, Long> updatedSeries = new HashMap<>();
        SeriesUpdater mockUpdater = updatedSeries::put;
        reopenedIndex.updateSeriesFromCommitData(mockUpdater);

        assertEquals(2, updatedSeries.size());
        assertEquals(100L, updatedSeries.get(1L).longValue());
        assertEquals(999L, updatedSeries.get(2L).longValue());

        reopenedIndex.close();
    }

    public void testSnapshotDeletionPolicy() throws IOException {
        Path tempDir = createTempDir("testSnapshotDeletionPolicy");
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(tempDir);
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k2", "v2"), 0L, 100L);
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k3", "v3"), 10L, 200L);
        liveSeriesIndex.commit();

        IndexCommit snapshot = liveSeriesIndex.snapshot();
        assertNotNull("Snapshot should not be null", snapshot);

        // Verify snapshot files exist
        Collection<String> snapshotFiles = snapshot.getFileNames();
        assertFalse("Snapshot should have files", snapshotFiles.isEmpty());
        Path indexDir = tempDir.resolve(LiveSeriesIndex.INDEX_DIR_NAME);
        for (String fileName : snapshotFiles) {
            assertTrue("Snapshot file should exist: " + fileName, Files.exists(indexDir.resolve(fileName)));
        }

        // Add more data and commit to create new files
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1", "k4", "v4"), 20L, 300L);
        liveSeriesIndex.commit();

        liveSeriesIndex.release(snapshot);

        // Verify some snapshot files may have been cleaned up (some files may still exist if they're shared with current commit)
        boolean someFilesCleanedUp = false;
        for (String fileName : snapshotFiles) {
            if (!Files.exists(indexDir.resolve(fileName))) {
                someFilesCleanedUp = true;
                break;
            }
        }
        assertTrue(someFilesCleanedUp);

        liveSeriesIndex.close();
    }

    // Helper method to get references matching a query
    private List<Long> getReferences(LiveSeriesIndex liveSeriesIndex, Query query) throws IOException {
        List<Long> refs = new ArrayList<>();

        ReaderManager liveReaderManager = liveSeriesIndex.getDirectoryReaderManager();
        DirectoryReader liveReader = null;
        try {
            liveReader = liveReaderManager.acquire();
            IndexSearcher liveSearcher = new IndexSearcher(liveReader);
            TopDocs topDocs = liveSearcher.search(query, Integer.MAX_VALUE);

            for (LeafReaderContext leaf : liveReader.leaves()) {
                NumericDocValues docValues = leaf.reader().getNumericDocValues(Constants.IndexSchema.REFERENCE);
                if (docValues == null) {
                    continue;
                }
                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docId = sd.doc;
                    if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                        int localDocId = docId - docBase;
                        if (docValues.advanceExact(localDocId)) {
                            refs.add(docValues.longValue());
                        }
                    }
                }
            }
        } finally {
            if (liveReader != null) {
                liveReaderManager.release(liveReader);
            }
        }

        return refs;
    }

    // Helper method to build a query for labels and timestamp range
    private Query buildQuery(String queryString, long minTimestamp, long maxTimestamp) {
        try {
            return new BooleanQuery.Builder().add(
                new QueryParser(Constants.IndexSchema.LABELS, new WhitespaceAnalyzer()).parse(queryString),
                BooleanClause.Occur.MUST
            )
                .add(LongPoint.newRangeQuery(Constants.IndexSchema.MIN_TIMESTAMP, Long.MIN_VALUE, maxTimestamp), BooleanClause.Occur.FILTER)
                .add(LongPoint.newRangeQuery(Constants.IndexSchema.MAX_TIMESTAMP, minTimestamp, Long.MAX_VALUE), BooleanClause.Occur.FILTER)
                .build();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void testConstructorIOException() throws IOException {
        Path invalidDir = createTempDir("testConstructorIOException");

        // Create a file where directory should be to cause IOException
        Path fileInsteadOfDir = invalidDir.resolve("subdir");
        Files.createFile(fileInsteadOfDir);

        expectThrows(FileSystemException.class, () -> { new LiveSeriesIndex(fileInsteadOfDir); });
    }

    public void testAddSeriesIOException() throws IOException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(createTempDir("testAddSeriesIOException"));

        // Close the index first to make addSeries throw IOException
        liveSeriesIndex.close();

        expectThrows(TSDBTragicException.class, () -> { liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1"), 0L, 100L); });
    }

    public void testLoadSeriesFromIndexIOException() throws IOException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(createTempDir("testLoadSeriesFromIndexIOException"));

        // Close the index first to make loadSeriesFromIndex throw IOException
        liveSeriesIndex.close();

        SeriesLoader mockLoader = series -> {};

        expectThrows(AlreadyClosedException.class, () -> { liveSeriesIndex.loadSeriesFromIndex(mockLoader); });
    }

    public void testCommitWithMetadataCommitException() throws IOException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(createTempDir("testCommitWithMetadataCommitException"));

        // Add some data
        liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1"), 0L, 100L);

        // Close the index to make commit fail
        liveSeriesIndex.close();

        MemSeries series = new MemSeries(0L, ByteLabels.fromStrings("k1", "v1"));
        series.setMaxSeqNo(100);

        expectThrows(AlreadyClosedException.class, () -> { liveSeriesIndex.commitWithMetadata(List.of(series)); });
    }

    /**
     * Test that TSDBTragicException is thrown when IndexWriter is closed.
     */
    public void testAddSeriesThrowsTragicException() throws IOException {
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(createTempDir("testTragicException"));

        // Close the index - this makes indexWriter.isOpen() == false
        liveSeriesIndex.close();

        // addSeries should throw TSDBTragicException
        assertThrows(TSDBTragicException.class, () -> liveSeriesIndex.addSeries(ByteLabels.fromStrings("k1", "v1"), 0L, 100L));
    }
}
