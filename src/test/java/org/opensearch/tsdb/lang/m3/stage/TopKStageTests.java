/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopKStageTests extends AbstractWireSerializingTestCase<TopKStage> {

    // ========== Constructor Tests ==========

    public void testConstructorWithAllParameters() {
        // Arrange & Act
        TopKStage topKStage = new TopKStage(5, SortByType.AVG, SortOrderType.ASC);

        // Assert
        assertEquals(5, topKStage.getK());
        assertEquals(SortByType.AVG, topKStage.getSortBy());
        assertEquals(SortOrderType.ASC, topKStage.getSortOrder());
        assertEquals("topK", topKStage.getName());
    }

    public void testConstructorWithKOnly() {
        // Arrange & Act
        TopKStage topKStage = new TopKStage(3);

        // Assert
        assertEquals(3, topKStage.getK());
        assertEquals(SortByType.CURRENT, topKStage.getSortBy()); // Default
        assertEquals(SortOrderType.DESC, topKStage.getSortOrder()); // Default
    }

    public void testConstructorWithDefaults() {
        // Arrange & Act
        TopKStage topKStage = new TopKStage();

        // Assert
        assertEquals(10, topKStage.getK()); // Default
        assertEquals(SortByType.CURRENT, topKStage.getSortBy()); // Default
        assertEquals(SortOrderType.DESC, topKStage.getSortOrder()); // Default
    }

    public void testConstructorWithInvalidK() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new TopKStage(0));
        assertEquals("K must be positive, got: 0", exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class, () -> new TopKStage(-1));
        assertEquals("K must be positive, got: -1", exception.getMessage());
    }

    public void testConstructorWithNullSortBy() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new TopKStage(5, null, SortOrderType.ASC));
        assertEquals("SortBy cannot be null", exception.getMessage());
    }

    public void testConstructorWithNullSortOrder() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new TopKStage(5, SortByType.AVG, null));
        assertEquals("SortOrder cannot be null", exception.getMessage());
    }

    // ========== Process Method Tests ==========

    public void testProcessWithEmptyInput() {
        // Arrange
        TopKStage topKStage = new TopKStage(5);
        List<TimeSeries> input = new ArrayList<>();

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert
        assertTrue("Empty input should return empty result", result.isEmpty());
    }

    public void testProcessWithNullInput() {
        TopKStage topKStage = new TopKStage(5);
        TestUtils.assertNullInputThrowsException(topKStage, "topK");
    }

    public void testProcessTopKWithCurrentDesc() {
        // Arrange: Create test series with different current values
        List<TimeSeries> input = createTestSeriesABC();
        TopKStage topKStage = new TopKStage(2, SortByType.CURRENT, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by current desc (B=3.0, C=2.0)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0))); // current=3.0
        assertEquals("C", getLabel(result.get(1))); // current=2.0
    }

    public void testProcessTopKWithCurrentAsc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        TopKStage topKStage = new TopKStage(2, SortByType.CURRENT, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by current asc (A=1.0, C=2.0)
        assertEquals(2, result.size());
        assertEquals("A", getLabel(result.get(0))); // current=1.0
        assertEquals("C", getLabel(result.get(1))); // current=2.0
    }

    public void testProcessTopKWithAvgDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        TopKStage topKStage = new TopKStage(2, SortByType.AVG, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by avg desc (B=2.0, C=1.5)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0))); // avg=2.0
        assertEquals("C", getLabel(result.get(1))); // avg=1.5
    }

    public void testProcessTopKWithSumDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        TopKStage topKStage = new TopKStage(2, SortByType.SUM, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by sum desc (B=6.0, C=3.0)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0))); // sum=6.0
        assertEquals("C", getLabel(result.get(1))); // sum=3.0
    }

    public void testProcessTopKWithMaxDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        TopKStage topKStage = new TopKStage(2, SortByType.MAX, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by max desc (B=3.0, C=2.0)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0))); // max=3.0
        assertEquals("C", getLabel(result.get(1))); // max=2.0
    }

    public void testProcessTopKWithMinAsc() {
        // Arrange: Create series with different minimum values
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 10.0)); // min=5.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 20.0)); // min=1.0
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(10.0, 15.0)); // min=10.0
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        TopKStage topKStage = new TopKStage(2, SortByType.MIN, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by min asc (B=1.0, A=5.0)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0))); // min=1.0
        assertEquals("A", getLabel(result.get(1))); // min=5.0
    }

    public void testProcessTopKWithStddevDesc() {
        // Arrange: Create series with different variations
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 5.0)); // stddev=0.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 10.0)); // stddev=high
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(4.0, 6.0)); // stddev=medium
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        TopKStage topKStage = new TopKStage(2, SortByType.STDDEV, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by stddev desc (B > C)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0))); // highest variation
        assertEquals("C", getLabel(result.get(1))); // medium variation
    }

    public void testProcessTopKWithNameAsc() {
        // Arrange
        TimeSeries seriesA = createTimeSeriesWithAlias("charlie", Arrays.asList(1.0));
        TimeSeries seriesB = createTimeSeriesWithAlias("alpha", Arrays.asList(2.0));
        TimeSeries seriesC = createTimeSeriesWithAlias("bravo", Arrays.asList(3.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        TopKStage topKStage = new TopKStage(2, SortByType.NAME, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return top 2 by name asc (alpha, bravo)
        assertEquals(2, result.size());
        assertEquals("alpha", getAlias(result.get(0)));
        assertEquals("bravo", getAlias(result.get(1)));
    }

    public void testProcessTopKWithKLargerThanInputSize() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC(); // 3 series
        TopKStage topKStage = new TopKStage(10); // k > input.size()

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return all series when k > input.size()
        assertEquals(3, result.size());
    }

    public void testProcessTopKWithSingleTimeSeries() {
        // Arrange
        List<TimeSeries> input = Arrays.asList(createLabeledTimeSeries("single", Arrays.asList(1.0, 2.0)));
        TopKStage topKStage = new TopKStage(3);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals("single", getLabel(result.get(0)));
    }

    // ========== Reduce Method Tests ==========

    public void testReduceWithMultipleShards() {
        // Arrange: Create TopKStage that will get top 3
        TopKStage topKStage = new TopKStage(3, SortByType.CURRENT, SortOrderType.DESC);

        // Shard 1: series with current values [5.0, 4.0]
        List<TimeSeries> shard1Series = Arrays.asList(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "shard1_high"), Arrays.asList(5.0)), // current=5.0
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "shard1_mid"), Arrays.asList(4.0))   // current=4.0
        );

        // Shard 2: series with current values [8.0, 6.0, 2.0]
        List<TimeSeries> shard2Series = Arrays.asList(
            StageTestUtils.createTimeSeries("ts3", Map.of("name", "shard2_highest"), Arrays.asList(8.0)), // current=8.0
            StageTestUtils.createTimeSeries("ts4", Map.of("name", "shard2_high"), Arrays.asList(6.0)),    // current=6.0
            StageTestUtils.createTimeSeries("ts5", Map.of("name", "shard2_low"), Arrays.asList(2.0))      // current=2.0
        );

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", shard1Series, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", shard2Series, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = Arrays.asList(agg1, agg2);

        // Act
        InternalAggregation result = topKStage.reduce(aggregations, true);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 3 series (the limit) with highest current values
        assertEquals(3, resultSeries.size());
        assertEquals("shard2_highest", resultSeries.get(0).getLabels().get("name")); // current=8.0
        assertEquals("shard2_high", resultSeries.get(1).getLabels().get("name"));    // current=6.0
        assertEquals("shard1_high", resultSeries.get(2).getLabels().get("name"));    // current=5.0
    }

    public void testReduceWithKGreaterThanTotalSeries() {
        // Arrange
        TopKStage topKStage = new TopKStage(10, SortByType.SUM, SortOrderType.DESC);

        List<TimeSeries> shard1Series = Arrays.asList(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), Arrays.asList(2.0, 3.0))   // sum=5.0
        );
        List<TimeSeries> shard2Series = Arrays.asList(
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), Arrays.asList(1.0, 1.0))   // sum=2.0
        );

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", shard1Series, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", shard2Series, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = Arrays.asList(agg1, agg2);

        // Act
        InternalAggregation result = topKStage.reduce(aggregations, true);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 2 series (less than k=10)
        assertEquals(2, resultSeries.size());
        assertEquals("series1", resultSeries.get(0).getLabels().get("name")); // higher sum first
        assertEquals("series2", resultSeries.get(1).getLabels().get("name"));
    }

    public void testReduceWithEmptyAggregations() {
        // Arrange
        TopKStage topKStage = new TopKStage(5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> topKStage.reduce(Collections.emptyList(), true)
        );
        assertEquals("Aggregations list cannot be null or empty", exception.getMessage());
    }

    public void testReduceWithNullAggregations() {
        // Arrange
        TopKStage topKStage = new TopKStage(5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> topKStage.reduce(null, true));
        assertEquals("Aggregations list cannot be null or empty", exception.getMessage());
    }

    public void testReduceWithAscendingOrder() {
        // Arrange: Test ascending order in reduce
        TopKStage topKStage = new TopKStage(2, SortByType.AVG, SortOrderType.ASC);

        List<TimeSeries> shard1Series = Arrays.asList(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "high_avg"), Arrays.asList(8.0, 10.0))   // avg=9.0
        );
        List<TimeSeries> shard2Series = Arrays.asList(
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "low_avg"), Arrays.asList(1.0, 3.0)),    // avg=2.0
            StageTestUtils.createTimeSeries("ts3", Map.of("name", "mid_avg"), Arrays.asList(4.0, 6.0))     // avg=5.0
        );

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", shard1Series, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", shard2Series, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = Arrays.asList(agg1, agg2);

        // Act
        InternalAggregation result = topKStage.reduce(aggregations, true);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should return top 2 with lowest averages (ascending order)
        assertEquals(2, resultSeries.size());
        assertEquals("low_avg", resultSeries.get(0).getLabels().get("name"));  // avg=2.0 (lowest)
        assertEquals("mid_avg", resultSeries.get(1).getLabels().get("name"));  // avg=5.0 (second lowest)
    }

    // ========== FromArgs Tests ==========

    public void testFromArgsWithAllParameters() {
        // Arrange
        Map<String, Object> args = Map.of(TopKStage.K_ARG, 5, TopKStage.SORT_BY_ARG, "avg", TopKStage.SORT_ORDER_ARG, "asc");

        // Act
        TopKStage topKStage = TopKStage.fromArgs(args);

        // Assert
        assertEquals(5, topKStage.getK());
        assertEquals(SortByType.AVG, topKStage.getSortBy());
        assertEquals(SortOrderType.ASC, topKStage.getSortOrder());
    }

    public void testFromArgsWithDefaults() {
        // Arrange
        Map<String, Object> args = Map.of();

        // Act
        TopKStage topKStage = TopKStage.fromArgs(args);

        // Assert
        assertEquals(10, topKStage.getK()); // Default
        assertEquals(SortByType.CURRENT, topKStage.getSortBy()); // Default
        assertEquals(SortOrderType.DESC, topKStage.getSortOrder()); // Default
    }

    public void testFromArgsWithNullArgs() {
        // Act
        TopKStage topKStage = TopKStage.fromArgs(null);

        // Assert: Should use all defaults
        assertEquals(10, topKStage.getK());
        assertEquals(SortByType.CURRENT, topKStage.getSortBy());
        assertEquals(SortOrderType.DESC, topKStage.getSortOrder());
    }

    public void testFromArgsWithPartialParameters() {
        // Arrange: Only specify k
        Map<String, Object> args = Map.of(TopKStage.K_ARG, 3);

        // Act
        TopKStage topKStage = TopKStage.fromArgs(args);

        // Assert
        assertEquals(3, topKStage.getK());
        assertEquals(SortByType.CURRENT, topKStage.getSortBy()); // Default
        assertEquals(SortOrderType.DESC, topKStage.getSortOrder()); // Default
    }

    public void testFromArgsWithStringK() {
        // Arrange
        Map<String, Object> args = Map.of(TopKStage.K_ARG, "7");

        // Act
        TopKStage topKStage = TopKStage.fromArgs(args);

        // Assert
        assertEquals(7, topKStage.getK());
    }

    public void testFromArgsWithInvalidStringK() {
        // Arrange
        Map<String, Object> args = Map.of(TopKStage.K_ARG, "invalid");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TopKStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for 'k' argument"));
    }

    public void testFromArgsWithInvalidKType() {
        // Arrange
        Map<String, Object> args = Map.of(TopKStage.K_ARG, Arrays.asList(1, 2, 3));

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TopKStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for 'k' argument"));
    }

    public void testFromArgsWithInvalidSortByType() {
        // Arrange
        Map<String, Object> args = Map.of(TopKStage.SORT_BY_ARG, 123);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TopKStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for 'sortBy' argument"));
    }

    public void testFromArgsWithInvalidSortOrderType() {
        // Arrange
        Map<String, Object> args = Map.of(TopKStage.SORT_ORDER_ARG, 456);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TopKStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for 'sortOrder' argument"));
    }

    public void testFromArgsWithNullValues() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(TopKStage.K_ARG, null);
        args.put(TopKStage.SORT_BY_ARG, null);
        args.put(TopKStage.SORT_ORDER_ARG, null);

        // Act
        TopKStage topKStage = TopKStage.fromArgs(args);

        // Assert: Should use defaults when values are null
        assertEquals(10, topKStage.getK());
        assertEquals(SortByType.CURRENT, topKStage.getSortBy());
        assertEquals(SortOrderType.DESC, topKStage.getSortOrder());
    }

    // ========== Interface Compliance Tests ==========

    public void testIsCoordinatorOnly() {
        TopKStage topKStage = new TopKStage(5);
        assertFalse("TopK should not be coordinator only to enable pushdown", topKStage.isCoordinatorOnly());
    }

    public void testIsGlobalAggregation() {
        TopKStage topKStage = new TopKStage(5);
        assertTrue("TopK should be a global aggregation", topKStage.isGlobalAggregation());
    }

    public void testSupportConcurrentSegmentSearch() {
        TopKStage topKStage = new TopKStage(5);
        assertTrue(topKStage.supportConcurrentSegmentSearch());
    }

    // ========== Serialization Tests ==========

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        TopKStage original = new TopKStage(7, SortByType.MAX, SortOrderType.ASC);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        original.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        TopKStage deserialized = TopKStage.readFrom(input);

        // Assert
        assertEquals(original.getK(), deserialized.getK());
        assertEquals(original.getSortBy(), deserialized.getSortBy());
        assertEquals(original.getSortOrder(), deserialized.getSortOrder());
    }

    public void testToXContent() throws IOException {
        // Arrange
        TopKStage stage = new TopKStage(5, SortByType.SUM, SortOrderType.DESC);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // Act
        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertTrue("JSON should contain k field", json.contains(TopKStage.K_ARG));
        assertTrue("JSON should contain sortBy field", json.contains(TopKStage.SORT_BY_ARG));
        assertTrue("JSON should contain sortOrder field", json.contains(TopKStage.SORT_ORDER_ARG));
        assertTrue("JSON should contain sum", json.contains("sum"));
        assertTrue("JSON should contain desc", json.contains("desc"));
        assertTrue("JSON should contain k value", json.contains("5"));
    }

    // ========== Equals and HashCode Tests ==========

    public void testEquals() {
        TopKStage stage1 = new TopKStage(5, SortByType.AVG, SortOrderType.DESC);
        TopKStage stage2 = new TopKStage(5, SortByType.AVG, SortOrderType.DESC);

        assertEquals("Equal TopKStages should be equal", stage1, stage2);

        TopKStage stageDiffK = new TopKStage(3, SortByType.AVG, SortOrderType.DESC);
        assertNotEquals("Different k should not be equal", stage1, stageDiffK);

        TopKStage stageDiffSortBy = new TopKStage(5, SortByType.MAX, SortOrderType.DESC);
        assertNotEquals("Different sortBy should not be equal", stage1, stageDiffSortBy);

        TopKStage stageDiffSortOrder = new TopKStage(5, SortByType.AVG, SortOrderType.ASC);
        assertNotEquals("Different sortOrder should not be equal", stage1, stageDiffSortOrder);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);
    }

    public void testHashCode() {
        TopKStage stage1 = new TopKStage(5, SortByType.AVG, SortOrderType.DESC);
        TopKStage stage2 = new TopKStage(5, SortByType.AVG, SortOrderType.DESC);

        assertEquals("Equal stages should have equal hash codes", stage1.hashCode(), stage2.hashCode());
    }

    // ========== Edge Cases Tests ==========

    public void testProcessWithEmptyTimeSeries() {
        // Arrange: Empty time series should not cause errors
        TopKStage topKStage = new TopKStage(3);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should return the series as-is
        assertEquals(1, result.size());
        assertEquals("empty", getLabel(result.get(0)));
    }

    public void testProcessWithNaNValues() {
        // Arrange: NaN values should be handled correctly during sorting
        TopKStage topKStage = new TopKStage(2, SortByType.AVG, SortOrderType.DESC);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 3.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = topKStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("test", getLabel(result.get(0)));
    }

    // ========== Helper Methods ==========

    /**
     * Creates a standard set of three test time series (A, B, C) for sorting tests.
     * Series A: [1.0, 1.0] -> avg=1.0, max=1.0, sum=2.0, current=1.0
     * Series B: [1.0, 2.0, 3.0] -> avg=2.0, max=3.0, sum=6.0, current=3.0
     * Series C: [1.0, 2.0] -> avg=1.5, max=2.0, sum=3.0, current=2.0
     */
    private List<TimeSeries> createTestSeriesABC() {
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(1.0, 1.0));
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 2.0, 3.0));
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(1.0, 2.0));
        return Arrays.asList(seriesA, seriesB, seriesC);
    }

    /**
     * Creates a time series with a label identifier for testing.
     */
    private TimeSeries createLabeledTimeSeries(String label, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.fromMap(Map.of("label", label));
        long endTime = values.isEmpty() ? 1000L : 1000L + (values.size() - 1) * 1000L;
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, label);
    }

    /**
     * Creates a time series with an alias for testing NAME sorting.
     */
    private TimeSeries createTimeSeriesWithAlias(String alias, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.emptyLabels();
        long endTime = values.isEmpty() ? 1000L : 1000L + (values.size() - 1) * 1000L;
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, alias);
    }

    /**
     * Extracts the label from a time series for verification purposes.
     */
    private String getLabel(TimeSeries timeSeries) {
        return timeSeries.getLabels().get("label");
    }

    /**
     * Extracts the alias from a time series for verification purposes.
     */
    private String getAlias(TimeSeries timeSeries) {
        return timeSeries.getAlias();
    }

    @Override
    protected Writeable.Reader<TopKStage> instanceReader() {
        return TopKStage::readFrom;
    }

    @Override
    protected TopKStage createTestInstance() {
        return new TopKStage(randomIntBetween(1, 100), randomFrom(SortByType.values()), randomFrom(SortOrderType.values()));
    }

    @Override
    protected TopKStage mutateInstance(TopKStage instance) {
        // Change one aspect of the instance to create a different but related instance
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new TopKStage(instance.getK() + 1, instance.getSortBy(), instance.getSortOrder());
            case 1 -> new TopKStage(
                instance.getK(),
                instance.getSortBy() == SortByType.AVG ? SortByType.SUM : SortByType.AVG,
                instance.getSortOrder()
            );
            case 2 -> new TopKStage(
                instance.getK(),
                instance.getSortBy(),
                instance.getSortOrder() == SortOrderType.ASC ? SortOrderType.DESC : SortOrderType.ASC
            );
            default -> throw new AssertionError();
        };
    }
}
