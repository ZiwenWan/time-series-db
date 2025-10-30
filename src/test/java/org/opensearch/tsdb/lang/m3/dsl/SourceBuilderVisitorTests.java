/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.dsl;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.common.ValueFilterType;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AbsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.BinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HistogramPercentilePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IsNonNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.KeepLastValuePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RemoveEmptyPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScaleToSecondsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SortPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SummarizePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TimeshiftPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ValueFilterPlanNode;
import org.opensearch.tsdb.lang.m3.stage.MovingStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Unit tests for SourceBuilderVisitor.
 */
public class SourceBuilderVisitorTests extends OpenSearchTestCase {

    private static final M3OSTranslator.Params DEFAULT_PARAMS = new M3OSTranslator.Params(
        Constants.Time.DEFAULT_TIME_UNIT,
        1000000L, // startTime
        2000000L, // endTime
        10000L,   // step
        true      // pushdown
    );

    private SourceBuilderVisitor visitor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        visitor = new SourceBuilderVisitor(DEFAULT_PARAMS);
    }

    /**
     * Test AbsPlanNode with correct number of children (1).
     */
    public void testAbsPlanNodeWithOneChild() {
        AbsPlanNode planNode = new AbsPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AbsPlanNode with incorrect number of children (0).
     */
    public void testAbsPlanNodeWithNoChildren() {
        AbsPlanNode planNode = new AbsPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AbsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AbsPlanNode with incorrect number of children (2).
     */
    public void testAbsPlanNodeWithTwoChildren() {
        AbsPlanNode planNode = new AbsPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AbsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AggregationPlanNode with correct number of children (1).
     */
    public void testAggregationPlanNodeWithOneChild() {
        AggregationPlanNode planNode = new AggregationPlanNode(1, AggregationType.SUM, List.of("tag1"));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AggregationPlanNode with incorrect number of children (0).
     */
    public void testAggregationPlanNodeWithNoChildren() {
        AggregationPlanNode planNode = new AggregationPlanNode(1, AggregationType.SUM, List.of("tag1"));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AggregationPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AggregationPlanNode with incorrect number of children (2).
     */
    public void testAggregationPlanNodeWithTwoChildren() {
        AggregationPlanNode planNode = new AggregationPlanNode(1, AggregationType.SUM, List.of("tag1"));
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AggregationPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AliasPlanNode with correct number of children (1).
     */
    public void testAliasPlanNodeWithOneChild() {
        AliasPlanNode planNode = new AliasPlanNode(1, "test_alias");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AliasPlanNode with incorrect number of children (0).
     */
    public void testAliasPlanNodeWithNoChildren() {
        AliasPlanNode planNode = new AliasPlanNode(1, "test_alias");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AliasPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AliasByTagsPlanNode with correct number of children (1).
     */
    public void testAliasByTagsPlanNodeWithOneChild() {
        AliasByTagsPlanNode planNode = new AliasByTagsPlanNode(1, List.of("tag1", "tag2"));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AliasByTagsPlanNode with incorrect number of children (0).
     */
    public void testAliasByTagsPlanNodeWithNoChildren() {
        AliasByTagsPlanNode planNode = new AliasByTagsPlanNode(1, List.of("tag1", "tag2"));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AliasByTagsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test BinaryPlanNode with correct number of children (2).
     */
    public void testBinaryPlanNodeWithTwoChildren() {
        BinaryPlanNode planNode = new BinaryPlanNode(1, BinaryPlanNode.Type.AS_PERCENT);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test BinaryPlanNode with incorrect number of children (1).
     */
    public void testBinaryPlanNodeWithOneChild() {
        BinaryPlanNode planNode = new BinaryPlanNode(1, BinaryPlanNode.Type.AS_PERCENT);
        planNode.addChild(createMockFetchNode(2));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("BinaryPlanNode must have exactly two children", exception.getMessage());
    }

    /**
     * Test BinaryPlanNode with incorrect number of children (3).
     */
    public void testBinaryPlanNodeWithThreeChildren() {
        BinaryPlanNode planNode = new BinaryPlanNode(1, BinaryPlanNode.Type.AS_PERCENT);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));
        planNode.addChild(createMockFetchNode(4));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("BinaryPlanNode must have exactly two children", exception.getMessage());
    }

    /**
     * Test KeepLastValuePlanNode with correct number of children (1).
     */
    public void testKeepLastValuePlanNodeWithOneChild() {
        KeepLastValuePlanNode planNode = new KeepLastValuePlanNode(1, "60s");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test KeepLastValuePlanNode with incorrect number of children (0).
     */
    public void testKeepLastValuePlanNodeWithNoChildren() {
        KeepLastValuePlanNode planNode = new KeepLastValuePlanNode(1, "60s");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("KeepLastValuePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test MovingPlanNode with correct number of children (1).
     */
    public void testMovingPlanNodeWithOneChild() {
        MovingPlanNode planNode = new MovingPlanNode(1, "60s", WindowAggregationType.AVG);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test MovingPlanNode with incorrect number of children (0).
     */
    public void testMovingPlanNodeWithNoChildren() {
        MovingPlanNode planNode = new MovingPlanNode(1, "60s", WindowAggregationType.AVG);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("MovingPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test SummarizePlanNode with correct number of children (1).
     */
    public void testSummarizePlanNodeWithOneChild() {
        SummarizePlanNode planNode = new SummarizePlanNode(1, "5m", WindowAggregationType.SUM, false);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test SummarizePlanNode with incorrect number of children (0).
     */
    public void testSummarizePlanNodeWithNoChildren() {
        SummarizePlanNode planNode = new SummarizePlanNode(1, "5m", WindowAggregationType.AVG, false);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("SummarizePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test SummarizePlanNode with alignToFrom=true.
     */
    public void testSummarizePlanNodeWithAlignToFrom() {
        SummarizePlanNode planNode = new SummarizePlanNode(1, "1h", WindowAggregationType.MAX, true);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test PerSecondPlanNode with correct number of children (1).
     */
    public void testPerSecondPlanNodeWithOneChild() {
        PerSecondPlanNode planNode = new PerSecondPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test PerSecondPlanNode with incorrect number of children (0).
     */
    public void testPerSecondPlanNodeWithNoChildren() {
        PerSecondPlanNode planNode = new PerSecondPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("PerSecondPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test SortPlanNode with correct number of children (1).
     */
    public void testSortPlanNodeWithOneChild() {
        SortPlanNode planNode = new SortPlanNode(1, "avg", "desc");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test SortPlanNode with incorrect number of children (0).
     */
    public void testSortPlanNodeWithNoChildren() {
        SortPlanNode planNode = new SortPlanNode(1, "value", "desc");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("SortPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test HistogramPercentilePlanNode with correct number of children (1).
     */
    public void testHistogramPercentilePlanNodeWithOneChild() {
        HistogramPercentilePlanNode planNode = new HistogramPercentilePlanNode(1, "bucket_id", "bucket", List.of(0.5f, 0.95f));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test HistogramPercentilePlanNode with incorrect number of children (0).
     */
    public void testHistogramPercentilePlanNodeWithNoChildren() {
        HistogramPercentilePlanNode planNode = new HistogramPercentilePlanNode(1, "bucket_id", "bucket", List.of(0.5f, 0.95f));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("HistogramPercentilePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test IsNonNullPlanNode with correct number of children (1).
     */
    public void testIsNonNullPlanNodeWithOneChild() {
        IsNonNullPlanNode planNode = new IsNonNullPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test IsNonNullPlanNode with incorrect number of children (0).
     */
    public void testIsNonNullPlanNodeWithNoChildren() {
        IsNonNullPlanNode planNode = new IsNonNullPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("IsNonNullPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test RemoveEmptyPlanNode with correct number of children (1).
     */
    public void testRemoveEmptyPlanNodeWithOneChild() {
        RemoveEmptyPlanNode planNode = new RemoveEmptyPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test RemoveEmptyPlanNode with incorrect number of children (0).
     */
    public void testRemoveEmptyPlanNodeWithNoChildren() {
        RemoveEmptyPlanNode planNode = new RemoveEmptyPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("RemoveEmptyPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test ScalePlanNode with correct number of children (1).
     */
    public void testScalePlanNodeWithOneChild() {
        ScalePlanNode planNode = new ScalePlanNode(1, 2.5);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ScalePlanNode with incorrect number of children (0).
     */
    public void testScalePlanNodeWithNoChildren() {
        ScalePlanNode planNode = new ScalePlanNode(1, 2.5);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ScalePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test ScaleToSecondsPlanNode with correct number of children (1).
     */
    public void testScaleToSecondsPlanNodeWithOneChild() {
        ScaleToSecondsPlanNode planNode = new ScaleToSecondsPlanNode(1, 10);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ScaleToSecondsPlanNode with incorrect number of children (0).
     */
    public void testScaleToSecondsPlanNodeWithNoChildren() {
        ScaleToSecondsPlanNode planNode = new ScaleToSecondsPlanNode(1, 10);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ScaleToSecondsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test TimeshiftPlanNode with correct number of children (1).
     */
    public void testTimeshiftPlanNodeWithOneChild() {
        TimeshiftPlanNode planNode = new TimeshiftPlanNode(1, "1d");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test TimeshiftPlanNode with incorrect number of children (0).
     */
    public void testTimeshiftPlanNodeWithNoChildren() {
        TimeshiftPlanNode planNode = new TimeshiftPlanNode(1, "1d");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("TimeshiftPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test that negative timeshift duration works correctly for fetch time range adjustment.
     * TimeshiftPlanNode.getDuration() converts negative to positive, so visitor gets positive value.
     */
    public void testTimeshiftPlanNodeWithNegativeDurationAdjustsFetchTimeRange() {
        TimeshiftPlanNode planNode = new TimeshiftPlanNode(1, "-2h");
        planNode.addChild(createMockFetchNode(2));

        SourceBuilderVisitor.ComponentHolder result = visitor.visit(planNode);
        SearchSourceBuilder builder = result.toSearchSourceBuilder();

        Collection<AggregationBuilder> aggregations = builder.aggregations().getAggregatorFactories();
        assertEquals(1, aggregations.size());

        TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggregations.iterator().next();

        // -2h = 7200000ms (converted to positive by TimeshiftPlanNode.getDuration())
        // startTime (1000000) - 7200000 = -6200000
        // endTime (2000000) - 7200000 = -5200000
        assertEquals("Start time should be adjusted by timeshift", -6200000L, unfoldAgg.getMinTimestamp());
        assertEquals("End time should be adjusted by timeshift", -5200000L, unfoldAgg.getMaxTimestamp());
    }

    /**
     * Test TransformNullPlanNode with correct number of children (1).
     */
    public void testTransformNullPlanNodeWithOneChild() {
        TransformNullPlanNode planNode = new TransformNullPlanNode(1, 0.0);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test TransformNullPlanNode with incorrect number of children (0).
     */
    public void testTransformNullPlanNodeWithNoChildren() {
        TransformNullPlanNode planNode = new TransformNullPlanNode(1, 0.0);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("TransformNullPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test UnionPlanNode with correct number of children (2).
     */
    public void testUnionPlanNodeWithTwoChildren() {
        UnionPlanNode planNode = new UnionPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test UnionPlanNode with correct number of children (3).
     */
    public void testUnionPlanNodeWithThreeChildren() {
        UnionPlanNode planNode = new UnionPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));
        planNode.addChild(createMockFetchNode(4));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test UnionPlanNode with incorrect number of children (1).
     */
    public void testUnionPlanNodeWithOneChild() {
        UnionPlanNode planNode = new UnionPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("UnionPlanNode must have at least two children", exception.getMessage());
    }

    /**
     * Test UnionPlanNode with incorrect number of children (0).
     */
    public void testUnionPlanNodeWithNoChildren() {
        UnionPlanNode planNode = new UnionPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("UnionPlanNode must have at least two children", exception.getMessage());
    }

    /**
     * Test ValueFilterPlanNode with correct number of children (1).
     */
    public void testValueFilterPlanNodeWithOneChild() {
        ValueFilterPlanNode planNode = new ValueFilterPlanNode(1, ValueFilterType.GE, 0.0);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ValueFilterPlanNode with incorrect number of children (0).
     */
    public void testValueFilterPlanNodeWithNoChildren() {
        ValueFilterPlanNode planNode = new ValueFilterPlanNode(1, ValueFilterType.GE, 0.0);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ValueFilterPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test FetchPlanNode with empty match filters.
     */
    public void testFetchPlanNodeWithEmptyMatchFilters() {
        Map<String, List<String>> emptyMatchFilters = Map.of();
        Map<String, List<String>> inverseMatchFilters = Map.of();
        FetchPlanNode planNode = new FetchPlanNode(1, emptyMatchFilters, inverseMatchFilters);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> visitor.visit(planNode));
        assertEquals("FetchPlanNode must have at least one match", exception.getMessage());
    }

    /**
     * Test that a valid FetchPlanNode with match filters works correctly.
     */
    public void testFetchPlanNodeWithValidMatchFilters() {
        FetchPlanNode planNode = createMockFetchNode(1);

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Push down is enabled by default and covered in {@link M3OSTranslator} tests.
     */
    public void testSkipPushdown() {
        MovingPlanNode movingPlanNode = new MovingPlanNode(1, "60s", WindowAggregationType.AVG);
        FetchPlanNode fetchPlanNode = createMockFetchNode(1);
        movingPlanNode.addChild(fetchPlanNode);

        SourceBuilderVisitor visitor = new SourceBuilderVisitor(
            new M3OSTranslator.Params(Constants.Time.DEFAULT_TIME_UNIT, 1000000L, 2000000L, 10000L, false)
        );

        SourceBuilderVisitor.ComponentHolder results = visitor.visit(movingPlanNode);
        SearchSourceBuilder builder = results.toSearchSourceBuilder();

        Collection<AggregationBuilder> aggregations = builder.aggregations().getAggregatorFactories();
        assertEquals(1, aggregations.size());
        assertTrue(aggregations.iterator().next() instanceof TimeSeriesUnfoldAggregationBuilder);
        TimeSeriesUnfoldAggregationBuilder timeSeriesUnfoldAggregationBuilder = (TimeSeriesUnfoldAggregationBuilder) aggregations.iterator()
            .next();
        assertNull("unfold aggregation should not contain stages with pushdown disabled", timeSeriesUnfoldAggregationBuilder.getStages());

        Collection<PipelineAggregationBuilder> pipelineAggregations = builder.aggregations().getPipelineAggregatorFactories();
        assertEquals(1, pipelineAggregations.size());
        assertTrue(pipelineAggregations.iterator().next() instanceof TimeSeriesCoordinatorAggregationBuilder);
        TimeSeriesCoordinatorAggregationBuilder timeSeriesCoordinatorAggregationBuilder =
            (TimeSeriesCoordinatorAggregationBuilder) pipelineAggregations.iterator().next();
        assertEquals(
            "coordinator aggregation contain stages with pushdown disabled",
            1,
            timeSeriesCoordinatorAggregationBuilder.getStages().size()
        );
        assertTrue(
            "the stage should be a moving stage",
            timeSeriesCoordinatorAggregationBuilder.getStages().getFirst() instanceof MovingStage
        );
    }

    public void testTimeUnit() throws IOException {
        MovingPlanNode movingPlanNode = new MovingPlanNode(1, "2m", WindowAggregationType.AVG);
        FetchPlanNode fetchPlanNode = createMockFetchNode(1);
        movingPlanNode.addChild(fetchPlanNode);

        SourceBuilderVisitor visitor = new SourceBuilderVisitor(
            new M3OSTranslator.Params(TimeUnit.SECONDS, 1000000L, 2000000L, 10000L, true)
        );

        SourceBuilderVisitor.ComponentHolder results = visitor.visit(movingPlanNode);
        SearchSourceBuilder builder = results.toSearchSourceBuilder();

        Collection<AggregationBuilder> aggregations = builder.aggregations().getAggregatorFactories();
        assertEquals(1, aggregations.size());
        assertTrue(aggregations.iterator().next() instanceof TimeSeriesUnfoldAggregationBuilder);
        TimeSeriesUnfoldAggregationBuilder timeSeriesUnfoldAggregationBuilder = (TimeSeriesUnfoldAggregationBuilder) aggregations.iterator()
            .next();
        assertEquals("unfold aggregation should contain the moving stage", 1, timeSeriesUnfoldAggregationBuilder.getStages().size());

        MovingStage movingStage = (MovingStage) timeSeriesUnfoldAggregationBuilder.getStages().getFirst();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            xContentBuilder.startObject();
            movingStage.toXContent(xContentBuilder, EMPTY_PARAMS);
            xContentBuilder.endObject();

            String json = xContentBuilder.toString();
            assertEquals("2h should be represented in seconds", "{\"interval\":120,\"function\":\"avg\"}", json);
        }
    }

    /**
     * Helper method to create a mock FetchPlanNode.
     */
    private FetchPlanNode createMockFetchNode(int id) {
        Map<String, List<String>> matchFilters = Map.of("__name__", List.of("test_metric"));
        Map<String, List<String>> inverseMatchFilters = Map.of();
        return new FetchPlanNode(id, matchFilters, inverseMatchFilters);
    }
}
