/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.MaxStage;
import org.opensearch.tsdb.lang.m3.stage.MinStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TimeSeriesStreamingAggregator and TimeSeriesStreamingAggregatorFactory.
 */
public class TimeSeriesStreamingAggregatorTests extends OpenSearchTestCase {

    // ---- Leaf pruning tests ----

    public void testGetLeafCollectorWithNonOverlappingTimeRange() throws IOException {
        long queryMin = 1000L;
        long queryMax = 5000L;
        long step = 100L;

        TimeSeriesStreamingAggregator aggregator = createStreamingAggregator(StreamingAggregationType.SUM, null, queryMin, queryMax, step);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(6000L, 10000L);
        LeafBucketCollector mockSub = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSub);
        assertSame("Should return sub-collector when leaf does not overlap time range", mockSub, result);

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    public void testGetLeafCollectorWithOverlappingTimeRange() throws IOException {
        long queryMin = 1000L;
        long queryMax = 5000L;
        long step = 100L;

        TimeSeriesStreamingAggregator aggregator = createStreamingAggregator(StreamingAggregationType.SUM, null, queryMin, queryMax, step);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(2000L, 6000L);
        LeafBucketCollector mockSub = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSub);
        assertNotSame("Should return new collector when leaf overlaps time range", mockSub, result);
        assertNotNull("Should return a non-null collector", result);

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    // ---- buildEmptyAggregation ----

    public void testBuildEmptyAggregation() throws IOException {
        TimeSeriesStreamingAggregator aggregator = createStreamingAggregator(StreamingAggregationType.SUM, null, 1000L, 5000L, 100L);

        InternalAggregation emptyAgg = aggregator.buildEmptyAggregation();
        assertNotNull("Empty aggregation should not be null", emptyAgg);
        assertTrue("Empty aggregation should be InternalTimeSeries", emptyAgg instanceof InternalTimeSeries);

        InternalTimeSeries ts = (InternalTimeSeries) emptyAgg;
        assertEquals("Empty aggregation should have correct name", "test_streaming_agg", ts.getName());
        assertTrue("Empty aggregation should have empty series list", ts.getTimeSeries().isEmpty());

        aggregator.close();
    }

    // ---- createReduceStage ----

    public void testCreateReduceStageForEachType() throws IOException {
        // Test global aggregation (no groupByTags) for each type
        assertReduceStageType(StreamingAggregationType.SUM, null, SumStage.class);
        assertReduceStageType(StreamingAggregationType.MIN, null, MinStage.class);
        assertReduceStageType(StreamingAggregationType.MAX, null, MaxStage.class);
        assertReduceStageType(StreamingAggregationType.AVG, null, AvgStage.class);

        // Test with empty groupByTags (should still produce same stage types)
        assertReduceStageType(StreamingAggregationType.SUM, List.of(), SumStage.class);
        assertReduceStageType(StreamingAggregationType.MIN, List.of(), MinStage.class);
        assertReduceStageType(StreamingAggregationType.MAX, List.of(), MaxStage.class);
        assertReduceStageType(StreamingAggregationType.AVG, List.of(), AvgStage.class);

        // Test with groupByTags
        List<String> tags = List.of("host", "region");
        assertReduceStageType(StreamingAggregationType.SUM, tags, SumStage.class);
        assertReduceStageType(StreamingAggregationType.MIN, tags, MinStage.class);
        assertReduceStageType(StreamingAggregationType.MAX, tags, MaxStage.class);
        assertReduceStageType(StreamingAggregationType.AVG, tags, AvgStage.class);
    }

    private void assertReduceStageType(StreamingAggregationType type, List<String> groupByTags, Class<?> expectedStageClass)
        throws IOException {
        TimeSeriesStreamingAggregator aggregator = createStreamingAggregator(type, groupByTags, 1000L, 5000L, 100L);

        InternalAggregation[] results = aggregator.buildAggregations(new long[] { 0 });
        assertEquals(1, results.length);
        assertTrue(results[0] instanceof InternalTimeSeries);

        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertNotNull("Reduce stage should not be null", ts.getReduceStage());
        assertTrue(
            "Reduce stage for "
                + type
                + " should be "
                + expectedStageClass.getSimpleName()
                + " but was "
                + ts.getReduceStage().getClass().getSimpleName(),
            expectedStageClass.isInstance(ts.getReduceStage())
        );

        aggregator.close();
    }

    // ---- calculateTimeArraySize ----

    public void testCalculateTimeArraySize() throws IOException {
        // Use factory's getEstimatedTimeArraySize() which uses the same formula
        TimeSeriesStreamingAggregatorFactory factory = createFactory(StreamingAggregationType.SUM, null, 1000L, 5000L, 100L);
        // (5000 - 1000) / 100 + 1 = 41
        assertEquals(41, factory.getEstimatedTimeArraySize());

        // Edge case: single point
        TimeSeriesStreamingAggregatorFactory singlePointFactory = createFactory(StreamingAggregationType.SUM, null, 1000L, 1000L, 100L);
        // (1000 - 1000) / 100 + 1 = 1
        assertEquals(1, singlePointFactory.getEstimatedTimeArraySize());

        // Larger range
        TimeSeriesStreamingAggregatorFactory largeFactory = createFactory(StreamingAggregationType.SUM, null, 0L, 3600000L, 300000L);
        // (3600000 - 0) / 300000 + 1 = 13
        assertEquals(13, largeFactory.getEstimatedTimeArraySize());
    }

    // ---- Factory tests ----

    public void testSupportsConcurrentSegmentSearch() throws IOException {
        TimeSeriesStreamingAggregatorFactory factory = createFactory(StreamingAggregationType.SUM, null, 1000L, 5000L, 100L);
        assertTrue("Streaming aggregation factory should support concurrent segment search", factory.supportsConcurrentSegmentSearch());
    }

    public void testFactoryConfiguration() throws IOException {
        List<String> tags = List.of("host", "region");
        TimeSeriesStreamingAggregatorFactory factory = createFactory(StreamingAggregationType.AVG, tags, 2000L, 8000L, 500L);

        assertEquals(StreamingAggregationType.AVG, factory.getAggregationType());
        assertEquals(tags, factory.getGroupByTags());
        assertEquals(2000L, factory.getMinTimestamp());
        assertEquals(8000L, factory.getMaxTimestamp());
        assertEquals(500L, factory.getStep());
    }

    public void testIsGlobalAggregation() throws IOException {
        // null tags -> global
        TimeSeriesStreamingAggregatorFactory nullTagsFactory = createFactory(StreamingAggregationType.SUM, null, 1000L, 5000L, 100L);
        assertTrue("null groupByTags should be global aggregation", nullTagsFactory.isGlobalAggregation());

        // empty tags -> global
        TimeSeriesStreamingAggregatorFactory emptyTagsFactory = createFactory(StreamingAggregationType.SUM, List.of(), 1000L, 5000L, 100L);
        assertTrue("empty groupByTags should be global aggregation", emptyTagsFactory.isGlobalAggregation());

        // non-empty tags -> not global
        TimeSeriesStreamingAggregatorFactory tagFactory = createFactory(StreamingAggregationType.SUM, List.of("host"), 1000L, 5000L, 100L);
        assertFalse("non-empty groupByTags should not be global aggregation", tagFactory.isGlobalAggregation());
    }

    // ---- Helper methods ----

    private TimeSeriesStreamingAggregator createStreamingAggregator(
        StreamingAggregationType type,
        List<String> groupByTags,
        long min,
        long max,
        long step
    ) throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);

        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");

        when(mockSearchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        when(mockSearchContext.bigArrays()).thenReturn(bigArrays);

        return new TimeSeriesStreamingAggregator(
            "test_streaming_agg",
            AggregatorFactories.EMPTY,
            type,
            groupByTags,
            mockSearchContext,
            null,
            CardinalityUpperBound.NONE,
            min,
            max,
            step,
            Map.of()
        );
    }

    private TimeSeriesStreamingAggregatorFactory createFactory(
        StreamingAggregationType type,
        List<String> groupByTags,
        long min,
        long max,
        long step
    ) throws IOException {
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);

        return new TimeSeriesStreamingAggregatorFactory(
            "test_factory",
            mockQueryShardContext,
            null,
            new AggregatorFactories.Builder(),
            Map.of(),
            type,
            groupByTags,
            min,
            max,
            step
        );
    }

    // ---- TSDBLeafReader mock infrastructure (reused from TimeSeriesUnfoldAggregatorTests) ----

    private static class TSDBLeafReaderWithContext {
        final TSDBLeafReader reader;
        final LeafReaderContext context;
        final DirectoryReader directoryReader;
        final Directory directory;
        final IndexWriter indexWriter;

        TSDBLeafReaderWithContext(
            TSDBLeafReader reader,
            LeafReaderContext context,
            DirectoryReader directoryReader,
            Directory directory,
            IndexWriter indexWriter
        ) {
            this.reader = reader;
            this.context = context;
            this.directoryReader = directoryReader;
            this.directory = directory;
            this.indexWriter = indexWriter;
        }
    }

    private TSDBLeafReaderWithContext createMockTSDBLeafReaderWithContext(long minTimestamp, long maxTimestamp) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());
        indexWriter.addDocument(new Document());
        indexWriter.commit();

        DirectoryReader tempReader = DirectoryReader.open(indexWriter);
        LeafReader baseReader = tempReader.leaves().get(0).reader();

        TSDBLeafReader tsdbLeafReader = new TSDBLeafReader(baseReader, minTimestamp, maxTimestamp) {
            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return reader;
            }

            @Override
            public TSDBDocValues getTSDBDocValues() throws IOException {
                return mock(TSDBDocValues.class);
            }

            @Override
            public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return List.of();
            }

            @Override
            public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return mock(Labels.class);
            }
        };

        CompositeReader compositeReader = new CompositeReader() {
            @Override
            protected List<? extends LeafReader> getSequentialSubReaders() {
                return Collections.singletonList(tsdbLeafReader);
            }

            @Override
            public TermVectors termVectors() throws IOException {
                return tsdbLeafReader.termVectors();
            }

            @Override
            public int numDocs() {
                return tsdbLeafReader.numDocs();
            }

            @Override
            public int maxDoc() {
                return tsdbLeafReader.maxDoc();
            }

            @Override
            public StoredFields storedFields() throws IOException {
                return tsdbLeafReader.storedFields();
            }

            @Override
            protected void doClose() throws IOException {}

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public int docFreq(Term term) throws IOException {
                return tsdbLeafReader.docFreq(term);
            }

            @Override
            public long totalTermFreq(Term term) throws IOException {
                return tsdbLeafReader.totalTermFreq(term);
            }

            @Override
            public long getSumDocFreq(String field) throws IOException {
                return tsdbLeafReader.getSumDocFreq(field);
            }

            @Override
            public int getDocCount(String field) throws IOException {
                return tsdbLeafReader.getDocCount(field);
            }

            @Override
            public long getSumTotalTermFreq(String field) throws IOException {
                return tsdbLeafReader.getSumTotalTermFreq(field);
            }
        };

        LeafReaderContext context = compositeReader.leaves().getFirst();
        return new TSDBLeafReaderWithContext(tsdbLeafReader, context, tempReader, directory, indexWriter);
    }
}
