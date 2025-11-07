/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.mockito.Mockito;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.TSDBEmptyLabelException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.InMemoryMetadataStore;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.compaction.NoopCompaction;
import org.opensearch.tsdb.core.index.closed.ClosedChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.utils.Constants;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doReturn;

public class HeadTests extends OpenSearchTestCase {

    private static final TimeValue TEST_CHUNK_EXPIRY = TimeValue.timeValueMinutes(30);
    private static final long TEST_BLOCK_DURATION = Duration.ofHours(2).toMillis();

    private final Settings defaultSettings = Settings.builder()
        .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), TimeValue.timeValueHours(2))
        .put(TSDBPlugin.TSDB_ENGINE_CHUNK_EXPIRY.getKey(), TEST_CHUNK_EXPIRY)
        .put(TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.getKey(), 120)
        .put(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.getKey(), Constants.Time.DEFAULT_TIME_UNIT.toString())
        .build();

    ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(
            TSDBPlugin.MGMT_THREAD_POOL_NAME,
            new FixedExecutorBuilder(Settings.builder().build(), TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 1, "")
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testHeadLifecycle() throws IOException, InterruptedException {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            defaultSettings
        );

        Head head = new Head(
            createTempDir("testHeadLifecycle"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            defaultSettings
        );
        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(1000, 10));
        Labels seriesLabels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        List<Long> expectedTimestamps = new ArrayList<>();
        List<Double> expectedValues = new ArrayList<>();

        // three batches create three chunks, with [8, 8, 2] samples respectively
        int sample = 1;
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 6; i++) {
                expectedTimestamps.add((long) sample);
                expectedValues.add((double) i);

                Head.HeadAppender appender = head.newAppender();
                appender.preprocess(0, 0, seriesLabels, sample++, i, () -> {});
                appender.appendSample(context, () -> {}, () -> {});
            }
        }

        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefreshBlocking();

        head.closeHeadChunks();
        closedChunkIndexManager.getReaderManagers().forEach(rm -> {
            try {
                rm.maybeRefreshBlocking();
            } catch (IOException e) {
                fail("Failed to refresh ClosedChunkIndexManager ReaderManager: " + e.getMessage());
            }
        });

        // Verify LiveSeriesIndex ReaderManager is accessible
        assertNotNull(head.getLiveSeriesIndex().getDirectoryReaderManager());

        // Verify ClosedChunkIndexManager ReaderManagers are accessible
        List<ReaderManager> readerManagers = closedChunkIndexManager.getReaderManagers();
        assertFalse(readerManagers.isEmpty());

        List<Object> seriesChunks = getChunks(head, closedChunkIndexManager);
        assertEquals(3, seriesChunks.size());

        assertTrue("First chunk is closed", seriesChunks.get(0) instanceof ClosedChunk);
        assertTrue("Second chunk is closed", seriesChunks.get(1) instanceof ClosedChunk);
        assertTrue("Third chunk is still in-memory", seriesChunks.get(2) instanceof MemChunk);

        ChunkIterator firstChunk = ((ClosedChunk) seriesChunks.get(0)).getChunkIterator();
        ChunkIterator secondChunk = ((ClosedChunk) seriesChunks.get(1)).getChunkIterator();
        Chunk thirdChunk = ((MemChunk) seriesChunks.get(2)).getChunk();

        assertEquals(thirdChunk.numSamples(), 2);

        List<Long> actualTimestamps = new ArrayList<>();
        List<Double> actualValues = new ArrayList<>();
        appendIterator(firstChunk, actualTimestamps, actualValues);
        assertEquals("First chunk should have 8 samples", 8, actualTimestamps.size());
        appendIterator(secondChunk, actualTimestamps, actualValues);
        assertEquals("First + second chunk should have 16 samples", 16, actualTimestamps.size());
        appendChunk(thirdChunk, actualTimestamps, actualValues);
        assertEquals("All three chunks should have 18 samples", 18, actualTimestamps.size());

        assertEquals(expectedTimestamps, actualTimestamps);
        assertEquals(expectedValues, actualValues);

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadSeriesCleanup() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(1000, 10));
        Labels seriesNoData = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels seriesWithData = ByteLabels.fromStrings("k1", "v1", "k3", "v3");

        head.getOrCreateSeries(seriesNoData.stableHash(), seriesNoData, 0L);
        assertEquals("One series in the series map", 1, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 1, head.getNumSeries());
        for (int i = 0; i < 10; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(i, seriesWithData.stableHash(), seriesWithData, i * 100L, i * 10.0, () -> {});
            appender.appendSample(context, () -> {}, () -> {});
        }

        assertEquals("getNumSeries returns 2", 2, head.getNumSeries());
        assertNotNull("Series with last append at seqNo 0 exists", head.getSeriesMap().getByReference(seriesNoData.stableHash()));
        assertNotNull("Series with last append at seqNo 10 exists", head.getSeriesMap().getByReference(seriesWithData.stableHash()));

        // Two chunks were created, minSeqNo of all in-memory chunks is >0 but <9
        head.closeHeadChunks();
        assertNull("Series with last append at seqNo 0 is removed", head.getSeriesMap().getByReference(seriesNoData.stableHash()));
        assertNotNull("Series with last append at seqNo 9 exists", head.getSeriesMap().getByReference(seriesWithData.stableHash()));
        assertEquals("One series remain in the series map", 1, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 1, head.getNumSeries());

        // Simulate advancing the time, so the series with data may have it's last chunk closed
        head.updateMaxSeenTimestamp(TEST_CHUNK_EXPIRY.getMillis() + 1000L);
        head.closeHeadChunks();
        assertNull("Series with last append at seqNo 9 is removed", head.getSeriesMap().getByReference(seriesWithData.stableHash()));
        assertTrue("No series remain in LiveSeriesIndex", getChunks(head, closedChunkIndexManager).isEmpty());
        assertEquals("No series remain in the series map", 0, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 0, head.getNumSeries());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadRecovery() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Path headPath = createTempDir("testHeadRecovery");

        Head head = new Head(headPath, new ShardId("headTest", "headTestUid", 0), closedChunkIndexManager, defaultSettings);
        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(1001, 10));
        Labels series1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels series2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        long series1Reference = 1L;
        long series2Reference = 2L;

        for (int i = 0; i < 12; i++) {
            Head.HeadAppender appender1 = head.newAppender();
            appender1.preprocess(i, series1Reference, series1, 100 * (i + 1), 10 * i, () -> {});
            appender1.appendSample(context, () -> {}, () -> {});

            Head.HeadAppender appender2 = head.newAppender();
            appender2.preprocess(i, series2Reference, series2, 10 * (i + 1), 10 * i, () -> {});
            appender2.appendSample(context, () -> {}, () -> {});
        }

        // 10 samples per chunk, so closing head chunks at 12 should leave 2 in-memory in the live chunk
        long minSeqNo = head.closeHeadChunks();
        assertEquals("10 samples were MMAPed, replay from minSeqNo + 1", 9, minSeqNo);
        head.close();
        closedChunkIndexManager.close();

        ClosedChunkIndexManager newClosedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head newHead = new Head(headPath, new ShardId("headTest", "headTestUid", 0), newClosedChunkIndexManager, defaultSettings);

        // MemSeries are correctly loaded and updated from commit data
        assertEquals(series1, newHead.getSeriesMap().getByReference(series1Reference).getLabels());
        assertEquals(series2, newHead.getSeriesMap().getByReference(series2Reference).getLabels());
        assertEquals(1000, newHead.getSeriesMap().getByReference(series1Reference).getMaxMMapTimestamp());
        assertEquals(100, newHead.getSeriesMap().getByReference(series2Reference).getMaxMMapTimestamp());
        assertEquals(11, newHead.getSeriesMap().getByReference(series1Reference).getMaxSeqNo());
        assertEquals(11, newHead.getSeriesMap().getByReference(series2Reference).getMaxSeqNo());

        // The translog replay correctly skips MMAPed samples
        int i = 0;
        while (i < 10) {
            Head.HeadAppender appender1 = newHead.newAppender();
            appender1.preprocess(i, series1Reference, series1, 100L * (i + 1), 10 * i, () -> {});
            assertFalse("Previously MMAPed sample is not appended again", appender1.appendSample(context, () -> {}, () -> {}));

            Head.HeadAppender appender2 = newHead.newAppender();
            appender2.preprocess(i, series2Reference, series2, 10L * (i + 1), 10 * i, () -> {});
            assertFalse("Previously MMAPed sample is not appended again", appender2.appendSample(context, () -> {}, () -> {}));
            i++;
        }

        // non MMAPed samples are appended
        while (i < 12) {
            Head.HeadAppender appender1 = newHead.newAppender();
            appender1.preprocess(i, series1Reference, series1, 100L * (i + 1), 10 * i, () -> {});
            assertTrue("Previously in-memory sample is appended", appender1.appendSample(context, () -> {}, () -> {}));

            Head.HeadAppender appender2 = newHead.newAppender();
            appender2.preprocess(i, series2Reference, series2, 10L * (i + 1), 10 * i, () -> {});
            assertTrue("Previously in-memory sample is appended", appender2.appendSample(context, () -> {}, () -> {}));
            i++;
        }

        newHead.close();
        newClosedChunkIndexManager.close();
    }

    public void testHeadRecoveryWithFailedChunks() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        MetadataStore metadataStore = new InMemoryMetadataStore();

        ClosedChunkIndexManager closedChunkIndexManager = Mockito.spy(
            new ClosedChunkIndexManager(
                metricsPath,
                metadataStore,
                new NOOPRetention(),
                new NoopCompaction(),
                threadPool,
                shardId,
                defaultSettings
            )
        );
        Path headPath = createTempDir("testHeadRecoveryWithCompaction");

        // Create 4 chunks across 4 indexes for series A.
        long chunkRange = TEST_BLOCK_DURATION;
        int samplesPerChunk = 8;
        long step = TEST_BLOCK_DURATION / samplesPerChunk;
        Head head = new Head(headPath, new ShardId("headTest", "headTestUid", 0), closedChunkIndexManager, defaultSettings);
        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(chunkRange, samplesPerChunk));
        Labels series1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long series1Reference = 1L;
        var seqNo = 0;
        for (int i = 0; i < 32; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(seqNo++, series1Reference, series1, step * (i + 1), 10 * i, () -> {});
            appender.appendSample(context, () -> {}, () -> {});
        }

        // Validate 4 indexes are created and closeHeadChunks return correct minSeq.
        var minSeqNo = head.closeHeadChunks();
        assertEquals(30, minSeqNo);
        assertEquals(4, closedChunkIndexManager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now()).size());

        // Create 8 chunks across 8 indexes for series B
        Labels series2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        long series2Reference = 2L;
        for (int i = 0; i < 64; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(seqNo++, series2Reference, series2, step * (i + 1), 10 * i, () -> {});
            appender.appendSample(context, () -> {}, () -> {});
        }

        // Setup to stop flushing of chunks at second chunk
        var memSeries2 = head.getSeriesMap().getByReference(series2Reference);
        var closableChunks = memSeries2.getClosableChunks(Instant.now().toEpochMilli(), TEST_CHUNK_EXPIRY.getMillis()).closableChunks();
        doReturn(false).when(closedChunkIndexManager).addMemChunk(memSeries2, closableChunks.get(1));

        // minSeq should increase by 8 (31+7)
        minSeqNo = head.closeHeadChunks();
        assertEquals(38, minSeqNo);
        head.close();

        // Reload the index should reinstate the previous state.
        ClosedChunkIndexManager newClosedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head newHead = new Head(headPath, new ShardId("headTest", "headTestUid", 0), newClosedChunkIndexManager, defaultSettings);

        // MemSeries are correctly loaded and updated from commit data
        assertEquals(series2, newHead.getSeriesMap().getByReference(series2Reference).getLabels());
        assertEquals(step * 7, newHead.getSeriesMap().getByReference(series2Reference).getMaxMMapTimestamp());
        assertEquals(95, newHead.getSeriesMap().getByReference(series2Reference).getMaxSeqNo());

        newHead.close();
        newClosedChunkIndexManager.close();
    }

    public void testHeadGetOrCreateSeries() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeries");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        // Test creating a new series with specified hash
        Head.SeriesResult result1 = head.getOrCreateSeries(123L, labels, 100L);
        assertTrue(result1.created());
        assertNotNull(result1.series());
        assertEquals(123L, result1.series().getReference());
        assertEquals(labels, result1.series().getLabels());

        // Test created the same series again - should return existing rather than creating
        Head.SeriesResult result2 = head.getOrCreateSeries(123L, labels, 200L);
        assertFalse(result2.created());
        assertEquals(result1.series(), result2.series());
        assertEquals(123L, result2.series().getReference());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadGetOrCreateSeriesHandlesHashFunctionChange() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeries");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        // Test creating a new series with specified hash
        Head.SeriesResult result1 = head.getOrCreateSeries(123L, labels, 100L);
        assertTrue(result1.created());
        assertNotNull(result1.series());
        assertEquals(123L, result1.series().getReference());
        assertEquals(labels, result1.series().getLabels());

        // Test created the same series again, but with another hash. Should result in two distinct series
        Head.SeriesResult result2 = head.getOrCreateSeries(labels.stableHash(), labels, 200L);
        assertTrue(result2.created());
        assertNotEquals(result1.series(), result2.series());
        assertEquals(labels.stableHash(), result2.series().getReference());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testGetOrCreateSeriesConcurrent() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeriesConcurrent");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
        long hash = 123L;

        // these values for # threads and iterations were chosen to reliably cause contention, based on code coverage inspection
        int numThreads = 50;
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            long currentHash = hash + iter;
            Labels currentLabels = ByteLabels.fromStrings("k1", "v1", "iteration", String.valueOf(iter));

            Head.SeriesResult[] results = new Head.SeriesResult[numThreads];
            Thread[] threads = new Thread[numThreads];
            CountDownLatch startLatch = new CountDownLatch(1);

            for (int i = 0; i < numThreads; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        startLatch.await();
                        results[threadId] = head.getOrCreateSeries(currentHash, currentLabels, 1000L + threadId);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            startLatch.countDown(); // attempt to start all threads at the same time, to ensure contention

            for (Thread thread : threads) {
                thread.join(5000);
            }

            assertEquals(
                "Only one series should exist for each hash",
                1,
                head.getSeriesMap().getSeriesMap().stream().mapToLong(MemSeries::getReference).filter(ref -> ref == currentHash).count()
            );

            MemSeries actualSeries = head.getSeriesMap().getByReference(currentHash);
            assertNotNull("Series should exist in map for each hash", actualSeries);

            int createdCount = 0;
            for (Head.SeriesResult result : results) {
                assertEquals("All threads should get the same series instance", actualSeries, result.series());
                if (result.created()) {
                    createdCount++;
                }
            }

            assertEquals("Exactly one thread should report creation", 1, createdCount);
        }

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test preprocess with concurrent threads and a single liveSeriesIndex.addSeries failure.
     * This simulates a race condition in seriesMap.putIfAbsent where a failed series might be deleted
     * from the map while another thread is trying to create it.
     */
    @SuppressForbidden(reason = "reflection usage is required here")
    public void testPreprocessConcurrentWithAddSeriesFailure() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testPreprocessConcurrentWithAddSeriesFailure");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );

        // Create a spy on LiveSeriesIndex to inject failures
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
        LiveSeriesIndex liveSeriesIndexSpy = Mockito.spy(head.getLiveSeriesIndex());

        // Use reflection to replace the liveSeriesIndex with the spy
        java.lang.reflect.Field liveSeriesIndexField = Head.class.getDeclaredField("liveSeriesIndex");
        liveSeriesIndexField.setAccessible(true);
        liveSeriesIndexField.set(head, liveSeriesIndexSpy);

        int numThreads = 50;
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            final int currentIter = iter;
            Labels currentLabels = ByteLabels.fromStrings("k1", "v1", "iteration", String.valueOf(currentIter));
            long currentHash = currentLabels.stableHash();

            // Introduce a single failure in addSeries for each iteration
            AtomicInteger callCount = new AtomicInteger(0);
            Mockito.doAnswer(invocation -> {
                if (callCount.incrementAndGet() == 1) {
                    // First call fails
                    throw new RuntimeException("Simulated addSeries failure");
                }
                // Subsequent calls succeed
                invocation.callRealMethod();
                return null;
            }).when(liveSeriesIndexSpy).addSeries(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());

            Boolean[] results = new Boolean[numThreads];
            Exception[] exceptions = new Exception[numThreads];
            Thread[] threads = new Thread[numThreads];
            CountDownLatch startLatch = new CountDownLatch(1);

            for (int i = 0; i < numThreads; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        startLatch.await();
                        Head.HeadAppender appender = head.newAppender();
                        results[threadId] = appender.preprocess(
                            currentIter * numThreads + threadId,
                            currentHash,
                            currentLabels,
                            1000L + threadId,
                            100.0 + threadId,
                            () -> {}
                        );
                    } catch (Exception e) {
                        exceptions[threadId] = e;
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            startLatch.countDown(); // Start all threads at the same time

            for (Thread thread : threads) {
                thread.join(5000);
            }

            // There are only 2 possible scenarios:
            // 1. A new series is created after the initial one is deleted (1 thread returns true from preprocess)
            // 2. No new series is created (0 threads return true from preprocess)

            // Check 1: Exactly 1 exception must be thrown (from the thread that hit addSeries failure)
            int exceptionCount = 0;
            for (int i = 0; i < numThreads; i++) {
                if (exceptions[i] != null) {
                    exceptionCount++;
                }
            }
            assertEquals("Exactly one thread must throw exception for iteration " + currentIter, 1, exceptionCount);

            // Check 2: Count how many threads returned true from preprocess (created series)
            int createdCount = 0;
            for (int i = 0; i < numThreads; i++) {
                if (results[i] != null && results[i]) {
                    createdCount++;
                }
            }
            assertTrue(
                "Either 0 or 1 thread should return true from preprocess for iteration " + currentIter,
                createdCount == 0 || createdCount == 1
            );

            // Check 3: If a thread returned true, verify series exists and is not marked failed
            if (createdCount == 1) {
                MemSeries series = head.getSeriesMap().getByReference(currentHash);
                assertNotNull("Series must exist if a thread returned true from preprocess for iteration " + currentIter, series);
                assertFalse("Series must not be marked as failed for iteration " + currentIter, series.isFailed());
            } else {
                assertNull(head.getSeriesMap().getByReference(currentHash));
            }
        }

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that markSeriesAsFailed removes the series from the LiveSeriesIndex.
     */
    public void testMarkSeriesAsFailed() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testMarkSeriesAsFailed");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );

        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        // Preprocess a new series successfully
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long hash = labels.stableHash();
        Head.HeadAppender appender = head.newAppender();
        boolean created = appender.preprocess(0, hash, labels, 1000L, 100.0, () -> {});

        assertTrue("Series should be created", created);
        MemSeries series = head.getSeriesMap().getByReference(hash);
        assertNotNull("Series should exist in seriesMap", series);

        // Refresh the reader to see the newly added series
        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefresh();

        // Verify series exists in LiveSeriesIndex by counting documents
        DirectoryReader readerBefore = head.getLiveSeriesIndex().getDirectoryReaderManager().acquire();
        try {
            int seriesCountBefore = readerBefore.numDocs();
            assertEquals("Should have 1 series in live index", 1, seriesCountBefore);
        } finally {
            head.getLiveSeriesIndex().getDirectoryReaderManager().release(readerBefore);
        }

        // Mark series as failed
        head.markSeriesAsFailed(series);

        // Verify series is marked as failed
        assertTrue("Series should be marked as failed", series.isFailed());

        // Verify series is deleted from seriesMap
        assertNull("Series should be removed from seriesMap", head.getSeriesMap().getByReference(hash));

        // Refresh the reader to see the deletion
        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefresh();

        // Verify series is deleted from LiveSeriesIndex by counting documents
        DirectoryReader readerAfter = head.getLiveSeriesIndex().getDirectoryReaderManager().acquire();
        try {
            int seriesCountAfter = readerAfter.numDocs();
            assertEquals("Should have 0 series in live index after marking as failed", 0, seriesCountAfter);
        } finally {
            head.getLiveSeriesIndex().getDirectoryReaderManager().release(readerAfter);
        }

        head.close();
        closedChunkIndexManager.close();
    }

    // Utility method to return all chunks from both LiveSeriesIndex and ClosedChunkIndexes
    private List<Object> getChunks(Head head, ClosedChunkIndexManager closedChunkIndexManager) throws IOException {
        List<Object> chunks = new ArrayList<>();

        // Query ClosedChunkIndexes
        List<ReaderManager> closedReaderManagers = closedChunkIndexManager.getReaderManagers();
        for (int i = 0; i < closedReaderManagers.size(); i++) {
            ReaderManager closedReaderManager = closedReaderManagers.get(i);
            DirectoryReader closedReader = null;
            try {
                closedReader = closedReaderManager.acquire();
                IndexSearcher closedSearcher = new IndexSearcher(closedReader);
                TopDocs topDocs = closedSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

                for (LeafReaderContext leaf : closedReader.leaves()) {
                    BinaryDocValues docValues = leaf.reader()
                        .getBinaryDocValues(org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK);
                    if (docValues == null) {
                        continue;
                    }
                    int docBase = leaf.docBase;
                    for (ScoreDoc sd : topDocs.scoreDocs) {
                        int docId = sd.doc;
                        if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                            int localDocId = docId - docBase;
                            if (docValues.advanceExact(localDocId)) {
                                BytesRef ref = docValues.binaryValue();
                                ClosedChunk chunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(ref);
                                chunks.add(chunk);
                            }
                        }
                    }
                }
            } finally {
                if (closedReader != null) {
                    closedReaderManager.release(closedReader);
                }
            }
        }

        // Query LiveSeriesIndex
        ReaderManager liveReaderManager = head.getLiveSeriesIndex().getDirectoryReaderManager();
        DirectoryReader liveReader = null;
        try {
            liveReader = liveReaderManager.acquire();
            IndexSearcher liveSearcher = new IndexSearcher(liveReader);
            TopDocs topDocs = liveSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

            for (LeafReaderContext leaf : liveReader.leaves()) {
                NumericDocValues docValues = leaf.reader()
                    .getNumericDocValues(org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE);
                if (docValues == null) {
                    continue;
                }
                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docId = sd.doc;
                    if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                        int localDocId = docId - docBase;
                        if (docValues.advanceExact(localDocId)) {
                            long ref = docValues.longValue();
                            MemSeries series = head.getSeriesMap().getByReference(ref);
                            MemChunk chunk = series.getHeadChunk();
                            while (chunk != null) {
                                chunks.add(chunk);
                                chunk = chunk.getPrev();
                            }
                        }
                    }
                }
            }
        } finally {
            if (liveReader != null) {
                liveReaderManager.release(liveReader);
            }
        }

        return chunks;
    }

    // Utility method to append all samples from a Chunk to lists
    private void appendChunk(Chunk chunk, List<Long> timestamps, List<Double> values) {
        appendIterator(chunk.iterator(), timestamps, values);
    }

    // Utility method to append all samples from a ChunkIterator to lists
    private void appendIterator(ChunkIterator iterator, List<Long> timestamps, List<Double> values) {
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            timestamps.add(tv.timestamp());
            values.add(tv.value());
        }
    }

    public void testNewAppender() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testNewAppender");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        // Test that newAppender returns non-null
        Head.HeadAppender appender1 = head.newAppender();
        assertNotNull("newAppender should return non-null instance", appender1);

        // Test that newAppender returns different instances
        Head.HeadAppender appender2 = head.newAppender();
        assertNotNull("second newAppender call should return non-null instance", appender2);
        assertNotSame("newAppender should return different instances", appender1, appender2);

        head.close();
        closedChunkIndexManager.close();
    }

    public void testSeriesCreatorThreadExecutesRunnableFirst() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testSeriesCreatorThread");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        // Use high thread count and iterations to reliably induce contention
        int numThreads = 50;
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            Labels labels = ByteLabels.fromStrings("k1", "v1", "iteration", String.valueOf(iter));
            long hash = labels.stableHash();

            CountDownLatch startLatch = new CountDownLatch(1);
            List<Boolean> createdResults = Collections.synchronizedList(new ArrayList<>());

            Thread[] threads = new Thread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        startLatch.await();
                        Head.HeadAppender appender = head.newAppender();
                        boolean created = appender.preprocess(threadId, hash, labels, 100L + threadId, 1.0, () -> {});
                        appender.append(() -> createdResults.add(created), () -> {});
                    } catch (InterruptedException e) {
                        fail("Thread was interrupted: " + e.getMessage());
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            startLatch.countDown(); // Start all threads simultaneously to ensure contention

            for (Thread thread : threads) {
                thread.join(5000);
            }

            assertTrue("First appender should create the series", createdResults.getFirst());
            for (int i = 1; i < numThreads; i++) {
                assertFalse("Subsequent appenders should not create the series", createdResults.get(i));
            }

            // Verify only one series was created
            MemSeries series = head.getSeriesMap().getByReference(hash);
            assertNotNull("Series should exist", series);
            assertEquals("One series created per iteration", iter + 1, head.getNumSeries());
        }

        head.close();
        closedChunkIndexManager.close();
    }

    public void testPreprocessFailure() throws IOException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Pass empty labels to trigger failure in preprocess
        Labels emptyLabels = ByteLabels.fromStrings(); // Empty labels
        long hash = 12345L;

        Head.HeadAppender appender = head.newAppender();

        // Expect exception for empty labels
        TSDBEmptyLabelException ex = assertThrows(
            TSDBEmptyLabelException.class,
            () -> appender.preprocess(0, hash, emptyLabels, 2000L, 100.0, () -> {})
        );
        assertTrue(ex.getMessage().contains("Labels"));

        head.close();
        closedChunkIndexManager.close();
    }

    public void testPreprocessFailureDeletesSeries() throws IOException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Close the live series index to trigger exception during series creation
        head.getLiveSeriesIndex().close();

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        Head.HeadAppender appender = head.newAppender();
        boolean[] failureCallbackCalled = { false };

        // Preprocess should fail when trying to add series to closed index
        // This will throw a TSDBTragicException (or RuntimeException wrapping it)
        assertThrows(
            RuntimeException.class,
            () -> appender.preprocess(0, hash, labels, 2000L, 100.0, () -> failureCallbackCalled[0] = true)
        );

        assertFalse("Failure callback should not be called for tragic exceptions", failureCallbackCalled[0]);

        MemSeries series = head.getSeriesMap().getByReference(hash);
        assertNull("Series should be deleted on failure", series);

        closedChunkIndexManager.close();
    }

    public void testAppendWithNullSeries() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Create appender but don't call preprocess (series will be null)
        Head.HeadAppender appender = head.newAppender();

        boolean[] successCalled = { false };
        boolean[] failureCalled = { false };

        // Call append without preprocess - should detect null series and call failureCallback
        assertThrows(RuntimeException.class, () -> appender.append(() -> successCalled[0] = true, () -> failureCalled[0] = true));

        assertFalse("Success callback should not be called", successCalled[0]);
        assertTrue("Failure callback should be called for null series", failureCalled[0]);

        head.close();
        closedChunkIndexManager.close();
    }

    public void testTranslogWriteFailure() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Create series successfully
        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        Head.HeadAppender appender = head.newAppender();
        boolean created = appender.preprocess(0, hash, labels, 2000L, 100.0, () -> {});
        assertTrue("Should create series", created);

        // Simulate callback failure - success callback throws exception
        boolean[] failureCalled = { false };
        boolean[] successCalled = { false };

        MemSeries series = head.getSeriesMap().getByReference(hash);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> appender.append(() -> {
            successCalled[0] = true;
            throw new RuntimeException("Simulated callback failure");
        }, () -> failureCalled[0] = true));
        assertTrue("Exception should mention callback failure", ex.getMessage().contains("Simulated callback failure"));

        assertTrue("Success callback should be called before throwing", successCalled[0]);
        assertTrue("Failure callback should be called when callback fails", failureCalled[0]);

        assertTrue("Series should be marked as failed after callback exception", series.isFailed());
        assertNull("Series should not exist", head.getSeriesMap().getByReference(hash));

        head.close();
        closedChunkIndexManager.close();
    }

    public void testAppendDetectsFailedSeries() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        // 1. Successfully preprocess and append first sample
        Head.HeadAppender appender1 = head.newAppender();
        boolean created1 = appender1.preprocess(0, hash, labels, 2000L, 100.0, () -> {});
        assertTrue("First appender should create series", created1);
        appender1.append(() -> {}, () -> {});

        // 2. For same series, preprocess next sample
        Head.HeadAppender appender2 = head.newAppender();
        boolean created2 = appender2.preprocess(1, hash, labels, 3000L, 200.0, () -> {});
        assertFalse("Second appender should not create series", created2);

        // 3. Before append, set series to failed
        MemSeries series = head.getSeriesMap().getByReference(hash);
        series.markFailed();

        // 4. Call append and expect failure callback to be called
        boolean[] successCalled = { false };
        boolean[] failureCalled = { false };

        assertThrows(RuntimeException.class, () -> appender2.append(() -> successCalled[0] = true, () -> failureCalled[0] = true));

        assertFalse("Success callback should not be called for failed series", successCalled[0]);
        assertTrue("Failure callback should be called for failed series", failureCalled[0]);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that a failed series gets replaced with a new series on retry.
     */
    public void testFailedSeriesGetsReplacedOnRetry() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        // First attempt - create series and mark it as failed
        Head.HeadAppender appender1 = head.newAppender();
        appender1.preprocess(0, hash, labels, 1000L, 100.0, () -> {});

        MemSeries series1 = head.getSeriesMap().getByReference(hash);
        assertNotNull("First series should be created", series1);
        head.markSeriesAsFailed(series1);

        // Second attempt - should create a new series replacing the failed one
        Head.HeadAppender appender2 = head.newAppender();
        boolean created = appender2.preprocess(1, hash, labels, 2000L, 200.0, () -> {});

        assertTrue("Second preprocess should create a new series", created);

        MemSeries series2 = head.getSeriesMap().getByReference(hash);
        assertNotNull("Series should exist after retry", series2);
        assertFalse("New series should not be marked as failed", series2.isFailed());
        assertNotSame("Should be a different series instance", series1, series2);

        head.close();
        closedChunkIndexManager.close();
    }
}
