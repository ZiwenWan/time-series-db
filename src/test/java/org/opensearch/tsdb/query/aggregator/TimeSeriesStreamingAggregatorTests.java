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
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.MaxStage;
import org.opensearch.tsdb.lang.m3.stage.MinStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.mockito.ArgumentMatchers.anyString;
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

    // ---- No-tag aggregation data processing tests ----

    public void testNoTagSumAggregation() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, null, min, max, step);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L, 3000L } },
            new double[][] { { 10.0, 20.0, 30.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(3, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(10.0, samples.getValue(0), 0.001);
        assertEquals(2000L, samples.getTimestamp(1));
        assertEquals(20.0, samples.getValue(1), 0.001);
        assertEquals(3000L, samples.getTimestamp(2));
        assertEquals(30.0, samples.getValue(2), 0.001);

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testNoTagMinAggregation() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.MIN, null, min, max, step);

        // Two chunks with overlapping timestamps
        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L }, { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 }, { 15.0, 5.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(10.0, samples.getValue(0), 0.001); // min(10.0, 15.0)
        assertEquals(2000L, samples.getTimestamp(1));
        assertEquals(5.0, samples.getValue(1), 0.001); // min(20.0, 5.0)

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testNoTagMaxAggregation() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.MAX, null, min, max, step);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L }, { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 }, { 15.0, 5.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(15.0, samples.getValue(0), 0.001); // max(10.0, 15.0)
        assertEquals(2000L, samples.getTimestamp(1));
        assertEquals(20.0, samples.getValue(1), 0.001); // max(20.0, 5.0)

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testNoTagAvgAggregation() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.AVG, null, min, max, step);

        // Two chunks with overlapping timestamps to accumulate sum + count
        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L }, { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 }, { 30.0, 40.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        // AVG path produces SumCountSamples — getValue() returns getAverage() (sum/count)
        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(20.0, samples.getValue(0), 0.001); // avg of 10,30 = 40/2
        assertEquals(2000L, samples.getTimestamp(1));
        assertEquals(30.0, samples.getValue(1), 0.001); // avg of 20,40 = 60/2

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testNoTagSumMultipleDocuments() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, null, min, max, step);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        // Collect two documents — each adds the same chunk data
        collector.collect(0, 0);
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(20.0, samples.getValue(0), 0.001); // 10+10
        assertEquals(2000L, samples.getTimestamp(1));
        assertEquals(40.0, samples.getValue(1), 0.001); // 20+20

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testNoTagAggregationSkipsNaNSamples() throws IOException {
        long min = 1000L, max = 5000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, null, min, max, step);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L, 3000L } },
            new double[][] { { 10.0, Double.NaN, 30.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        // NaN sample at timestamp 2000 should be skipped
        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(10.0, samples.getValue(0), 0.001);
        assertEquals(3000L, samples.getTimestamp(1));
        assertEquals(30.0, samples.getValue(1), 0.001);

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testNoTagAggregationEmptyResult() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, null, min, max, step);

        // Empty chunks — no data processed
        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] {},
            new double[][] {},
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        // hasData=false → empty result list
        assertTrue("Empty aggregation should have no time series", ts.getTimeSeries().isEmpty());

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    // ---- Tag-based aggregation data processing tests ----

    public void testTagSumAggregation() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        List<String> groupByTags = List.of("region");
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, groupByTags, min, max, step);

        Labels mockLabels = mock(Labels.class);
        when(mockLabels.has("region")).thenReturn(true);
        when(mockLabels.get("region")).thenReturn("us");

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 } },
            docId -> mockLabels
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(10.0, samples.getValue(0), 0.001);
        assertEquals(2000L, samples.getTimestamp(1));
        assertEquals(20.0, samples.getValue(1), 0.001);

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testTagAvgAggregation() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        List<String> groupByTags = List.of("region");
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.AVG, groupByTags, min, max, step);

        Labels mockLabels = mock(Labels.class);
        when(mockLabels.has("region")).thenReturn(true);
        when(mockLabels.get("region")).thenReturn("us");

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L }, { 1000L } },
            new double[][] { { 10.0 }, { 30.0 } },
            docId -> mockLabels
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        // AVG path: getValue returns getAverage() (sum/count)
        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(1, samples.size());
        assertEquals(1000L, samples.getTimestamp(0));
        assertEquals(20.0, samples.getValue(0), 0.001); // avg of 10,30 = 40/2

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testTagAggregationMissingGroupByTag() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        List<String> groupByTags = List.of("region");
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, groupByTags, min, max, step);

        // Labels missing the required "region" tag
        Labels mockLabels = mock(Labels.class);
        when(mockLabels.has("region")).thenReturn(false);

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 } },
            docId -> mockLabels
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        // Document skipped due to missing group-by tag → no results
        assertTrue("Missing group-by tag should produce no results", ts.getTimeSeries().isEmpty());

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testTagAggregationEmptyGroupByTags() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        // Empty groupByTags list → extractGroupLabels returns ByteLabels.emptyLabels()
        List<String> groupByTags = List.of();
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, groupByTags, min, max, step);

        // Note: with empty groupByTags, createStreamingState() creates NoTagStreamingState
        // (groupByTags.isEmpty() check in createStreamingState)
        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));
        collector.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals(1, ts.getTimeSeries().size());

        SampleList samples = ts.getTimeSeries().get(0).getSamples();
        assertEquals(2, samples.size());

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    public void testTagAggregationMultipleGroups() throws IOException {
        long min = 1000L, max = 4000L, step = 1000L;
        List<String> groupByTags = List.of("region");
        TimeSeriesStreamingAggregator agg = createStreamingAggregator(StreamingAggregationType.SUM, groupByTags, min, max, step);

        // Two different label sets for two different groups
        Labels usLabels = mock(Labels.class);
        when(usLabels.has("region")).thenReturn(true);
        when(usLabels.get("region")).thenReturn("us");

        Labels euLabels = mock(Labels.class);
        when(euLabels.has("region")).thenReturn(true);
        when(euLabels.get("region")).thenReturn("eu");

        // First leaf reader for "us" group
        TSDBLeafReaderWithContext readerCtx1 = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 } },
            docId -> usLabels
        );
        LeafBucketCollector collector1 = agg.getLeafCollector(readerCtx1.context, mock(LeafBucketCollector.class));
        collector1.collect(0, 0);

        // Second leaf reader for "eu" group
        TSDBLeafReaderWithContext readerCtx2 = createMockTSDBLeafReaderWithContext(
            min,
            max,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 30.0, 40.0 } },
            docId -> euLabels
        );
        LeafBucketCollector collector2 = agg.getLeafCollector(readerCtx2.context, mock(LeafBucketCollector.class));
        collector2.collect(0, 0);

        InternalAggregation[] results = agg.buildAggregations(new long[] { 0 });
        InternalTimeSeries ts = (InternalTimeSeries) results[0];
        assertEquals("Should have 2 groups", 2, ts.getTimeSeries().size());

        // Verify each group has 2 samples
        for (TimeSeries series : ts.getTimeSeries()) {
            assertEquals(2, series.getSamples().size());
        }

        readerCtx1.directoryReader.close();
        readerCtx1.directory.close();
        readerCtx2.directoryReader.close();
        readerCtx2.directory.close();
        agg.close();
    }

    // ---- Circuit breaker tests ----

    public void testCircuitBreakerTrips() throws IOException {
        ArmedCircuitBreaker armedBreaker = new ArmedCircuitBreaker("request");
        CircuitBreakerService circuitBreakerService = mock(CircuitBreakerService.class);
        when(circuitBreakerService.getBreaker(anyString())).thenReturn(armedBreaker);

        SearchContext mockSearchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        when(mockSearchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        when(mockSearchContext.bigArrays()).thenReturn(bigArrays);

        TimeSeriesStreamingAggregator agg = new TimeSeriesStreamingAggregator(
            "test_cb",
            AggregatorFactories.EMPTY,
            StreamingAggregationType.SUM,
            null,
            mockSearchContext,
            null,
            CardinalityUpperBound.NONE,
            1000L,
            4000L,
            1000L,
            Map.of()
        );

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(
            1000L,
            4000L,
            new long[][] { { 1000L, 2000L } },
            new double[][] { { 10.0, 20.0 } },
            docId -> mock(Labels.class)
        );

        LeafBucketCollector collector = agg.getLeafCollector(readerCtx.context, mock(LeafBucketCollector.class));

        // Arm the breaker after construction, so addCircuitBreakerBytes trips
        armedBreaker.arm();

        expectThrows(CircuitBreakingException.class, () -> collector.collect(0, 0));

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        agg.close();
    }

    // ---- StreamingAggregationType tests ----

    public void testGetDisplayName() {
        assertEquals("sum", StreamingAggregationType.SUM.getDisplayName());
        assertEquals("min", StreamingAggregationType.MIN.getDisplayName());
        assertEquals("max", StreamingAggregationType.MAX.getDisplayName());
        assertEquals("avg", StreamingAggregationType.AVG.getDisplayName());
    }

    // ---- Factory createInternal test ----

    public void testFactoryCreateInternal() throws IOException {
        TimeSeriesStreamingAggregatorFactory factory = createFactory(StreamingAggregationType.SUM, null, 1000L, 5000L, 100L);

        SearchContext mockSearchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        CircuitBreakerService cbs = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, cbs, "request");
        when(mockSearchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        when(mockSearchContext.bigArrays()).thenReturn(bigArrays);

        Aggregator aggregator = factory.createInternal(mockSearchContext, null, CardinalityUpperBound.NONE, Map.of());
        assertNotNull(aggregator);
        assertTrue("Should create TimeSeriesStreamingAggregator", aggregator instanceof TimeSeriesStreamingAggregator);
        aggregator.close();
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

    /**
     * Overloaded helper: creates a TSDBLeafReaderWithContext that returns provided chunk data and labels.
     * Each call to chunksForDoc creates fresh TestChunkIterator instances.
     */
    private TSDBLeafReaderWithContext createMockTSDBLeafReaderWithContext(
        long minTimestamp,
        long maxTimestamp,
        long[][] chunkTimestamps,
        double[][] chunkValues,
        IntFunction<Labels> labelProvider
    ) throws IOException {
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
                // Create fresh TestChunkIterator instances for each call
                List<ChunkIterator> chunks = new ArrayList<>();
                for (int i = 0; i < chunkTimestamps.length; i++) {
                    chunks.add(new TestChunkIterator(chunkTimestamps[i], chunkValues[i]));
                }
                return chunks;
            }

            @Override
            public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return labelProvider.apply(docId);
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

    /**
     * Simple ChunkIterator that yields predetermined samples from arrays.
     * The default decodeSamples() method works automatically via next()/at().
     */
    private static class TestChunkIterator implements ChunkIterator {
        private final long[] timestamps;
        private final double[] values;
        private int index = -1;

        TestChunkIterator(long[] timestamps, double[] values) {
            this.timestamps = timestamps;
            this.values = values;
        }

        @Override
        public ValueType next() {
            index++;
            return index < timestamps.length ? ValueType.FLOAT : ValueType.NONE;
        }

        @Override
        public TimestampValue at() {
            return new TimestampValue(timestamps[index], values[index]);
        }

        @Override
        public Exception error() {
            return null;
        }

        @Override
        public int totalSamples() {
            return timestamps.length;
        }
    }

    /**
     * Circuit breaker that can be armed after construction to test breaker trips.
     */
    private static class ArmedCircuitBreaker extends NoopCircuitBreaker {
        private boolean armed = false;

        ArmedCircuitBreaker(String name) {
            super(name);
        }

        void arm() {
            armed = true;
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (armed) {
                throw new CircuitBreakingException("Test circuit breaker tripped", bytes, 1000L, Durability.TRANSIENT);
            }
            return bytes;
        }
    }
}
