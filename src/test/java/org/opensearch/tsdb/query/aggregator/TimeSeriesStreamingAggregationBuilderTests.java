/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BaseAggregationBuilder;
import org.opensearch.search.aggregations.BaseAggregationTestCase;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TimeSeriesStreamingAggregationBuilder.
 */
public class TimeSeriesStreamingAggregationBuilderTests extends BaseAggregationTestCase<TimeSeriesStreamingAggregationBuilder> {

    @Override
    protected TimeSeriesStreamingAggregationBuilder createTestAggregatorBuilder() {
        StreamingAggregationType[] types = StreamingAggregationType.values();
        StreamingAggregationType aggregationType = types[randomInt(types.length - 1)];

        List<String> groupByTags = null;
        int tagChoice = randomInt(2);
        if (tagChoice == 1) {
            // Use null for empty tags (empty list doesn't survive XContent roundtrip
            // because internalXContent skips empty tags and parse() defaults to null)
            groupByTags = null;
        } else if (tagChoice == 2) {
            int tagCount = randomIntBetween(1, 3);
            List<String> tags = new ArrayList<>();
            for (int i = 0; i < tagCount; i++) {
                tags.add(randomAlphaOfLengthBetween(3, 10));
            }
            groupByTags = tags;
        }

        String name = randomAlphaOfLengthBetween(3, 20);
        long minTimestamp = randomLongBetween(0, 5000L);
        long maxTimestamp = randomLongBetween(minTimestamp + 1, minTimestamp + 5000L);
        long step = randomLongBetween(1L, 100L) * 10;

        return new TimeSeriesStreamingAggregationBuilder(name, aggregationType, groupByTags, minTimestamp, maxTimestamp, step);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(
                    BaseAggregationBuilder.class,
                    new ParseField(TimeSeriesStreamingAggregationBuilder.NAME),
                    (p, n) -> TimeSeriesStreamingAggregationBuilder.parse((String) n, p)
                )
            )
        );
    }

    @Override
    protected NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    AggregationBuilder.class,
                    TimeSeriesStreamingAggregationBuilder.NAME,
                    TimeSeriesStreamingAggregationBuilder::new
                )
            )
        );
    }

    // ---- Constructor tests ----

    public void testConstructorBasic() {
        TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder(
            "test_streaming",
            StreamingAggregationType.SUM,
            List.of("host"),
            1000L,
            2000L,
            100L
        );

        assertEquals("test_streaming", builder.getName());
        assertEquals(StreamingAggregationType.SUM, builder.getAggregationType());
        assertEquals(List.of("host"), builder.getGroupByTags());
        assertEquals(1000L, builder.getMinTimestamp());
        assertEquals(2000L, builder.getMaxTimestamp());
        assertEquals(100L, builder.getStep());
        assertEquals("time_series_streaming", builder.getType());
        assertEquals(TimeSeriesStreamingAggregationBuilder.NAME, "time_series_streaming");
    }

    public void testConstructorValidatesTimestampRange() {
        // maxTimestamp == minTimestamp
        IllegalArgumentException exception1 = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSeriesStreamingAggregationBuilder("test", StreamingAggregationType.SUM, null, 1000L, 1000L, 100L)
        );
        assertTrue(exception1.getMessage().contains("maxTimestamp must be greater than minTimestamp"));

        // maxTimestamp < minTimestamp
        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSeriesStreamingAggregationBuilder("test", StreamingAggregationType.SUM, null, 2000L, 1000L, 100L)
        );
        assertTrue(exception2.getMessage().contains("maxTimestamp must be greater than minTimestamp"));
    }

    public void testConstructorValidatesStep() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSeriesStreamingAggregationBuilder("test", StreamingAggregationType.SUM, null, 1000L, 2000L, 0L)
        );
        assertTrue(exception.getMessage().contains("step must be positive"));

        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSeriesStreamingAggregationBuilder("test", StreamingAggregationType.SUM, null, 1000L, 2000L, -1L)
        );
        assertTrue(exception2.getMessage().contains("step must be positive"));
    }

    public void testConstructorValidatesAggregationType() {
        expectThrows(NullPointerException.class, () -> new TimeSeriesStreamingAggregationBuilder("test", null, null, 1000L, 2000L, 100L));
    }

    // ---- Shallow copy ----

    public void testShallowCopy() {
        List<String> tags = List.of("host", "region");
        TimeSeriesStreamingAggregationBuilder original = new TimeSeriesStreamingAggregationBuilder(
            "test_copy",
            StreamingAggregationType.AVG,
            tags,
            1000L,
            2000L,
            100L
        );

        AggregationBuilder copy = original.shallowCopy(null, Map.of("key", "value"));
        assertTrue(copy instanceof TimeSeriesStreamingAggregationBuilder);
        TimeSeriesStreamingAggregationBuilder typedCopy = (TimeSeriesStreamingAggregationBuilder) copy;

        assertEquals(original.getName(), typedCopy.getName());
        assertEquals(original.getAggregationType(), typedCopy.getAggregationType());
        assertEquals(original.getGroupByTags(), typedCopy.getGroupByTags());
        assertEquals(original.getMinTimestamp(), typedCopy.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), typedCopy.getMaxTimestamp());
        assertEquals(original.getStep(), typedCopy.getStep());
    }

    // ---- Bucket cardinality ----

    public void testBucketCardinality() {
        TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder(
            "test_card",
            StreamingAggregationType.SUM,
            null,
            1000L,
            2000L,
            100L
        );
        assertEquals(AggregationBuilder.BucketCardinality.MANY, builder.bucketCardinality());
    }

    // ---- doBuild tests ----

    public void testDoBuild() throws Exception {
        TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder(
            "test_build",
            StreamingAggregationType.SUM,
            List.of("host"),
            1000L,
            2000L,
            100L
        );

        QueryShardContext mockContext = mockQueryShardContextWithTsdbEngine(true);
        AggregatorFactory mockParent = mock(AggregatorFactory.class);
        AggregatorFactories.Builder mockSubFactoriesBuilder = mock(AggregatorFactories.Builder.class);

        AggregatorFactory factory = builder.doBuild(mockContext, mockParent, mockSubFactoriesBuilder);
        assertNotNull(factory);
        assertTrue(factory instanceof TimeSeriesStreamingAggregatorFactory);
    }

    public void testDoBuildFailsWhenTsdbEngineDisabled() throws Exception {
        TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder(
            "test_build_fail",
            StreamingAggregationType.SUM,
            null,
            1000L,
            2000L,
            100L
        );

        QueryShardContext mockContext = mockQueryShardContextWithTsdbEngine(false);
        AggregatorFactory mockParent = mock(AggregatorFactory.class);
        AggregatorFactories.Builder mockSubFactoriesBuilder = mock(AggregatorFactories.Builder.class);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> builder.doBuild(mockContext, mockParent, mockSubFactoriesBuilder)
        );
        assertTrue(exception.getMessage().contains("tsdb_engine.enabled is true"));
    }

    // ---- XContent tests ----

    public void testXContentGeneration() throws IOException {
        TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder(
            "test_xcontent",
            StreamingAggregationType.SUM,
            List.of("host", "region"),
            1000L,
            2000L,
            100L
        );

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("aggregation_type"));
        assertTrue(jsonString.contains("sum"));
        assertTrue(jsonString.contains("group_by_tags"));
        assertTrue(jsonString.contains("min_timestamp"));
        assertTrue(jsonString.contains("max_timestamp"));
        assertTrue(jsonString.contains("step"));
    }

    public void testXContentWithoutGroupByTags() throws IOException {
        TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder(
            "test_xcontent_null",
            StreamingAggregationType.MIN,
            null,
            1000L,
            2000L,
            100L
        );

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertFalse("group_by_tags should be absent when null", jsonString.contains("group_by_tags"));
    }

    // ---- Parse tests ----

    public void testParseValidInput() throws Exception {
        String json = """
            {
              "aggregation_type": "sum",
              "group_by_tags": ["host", "region"],
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            TimeSeriesStreamingAggregationBuilder result = TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser);

            assertEquals("test_agg", result.getName());
            assertEquals(StreamingAggregationType.SUM, result.getAggregationType());
            assertEquals(List.of("host", "region"), result.getGroupByTags());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(100L, result.getStep());
        }
    }

    public void testParseMissingAggregationType() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser)
            );
            assertTrue(exception.getMessage().contains("aggregation_type"));
        }
    }

    public void testParseMissingMinTimestamp() throws Exception {
        String json = """
            {
              "aggregation_type": "sum",
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser)
            );
            assertTrue(exception.getMessage().contains("min_timestamp"));
        }
    }

    public void testParseMissingMaxTimestamp() throws Exception {
        String json = """
            {
              "aggregation_type": "sum",
              "min_timestamp": 1000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser)
            );
            assertTrue(exception.getMessage().contains("max_timestamp"));
        }
    }

    public void testParseMissingStep() throws Exception {
        String json = """
            {
              "aggregation_type": "sum",
              "min_timestamp": 1000,
              "max_timestamp": 2000
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser)
            );
            assertTrue(exception.getMessage().contains("step"));
        }
    }

    public void testParseInvalidAggregationType() throws Exception {
        String json = """
            {
              "aggregation_type": "invalid_type",
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser)
            );
            assertTrue(exception.getMessage().contains("Invalid aggregation_type"));
        }
    }

    public void testParseWithUnknownFields() throws Exception {
        String json = """
            {
              "unknown_field": "value",
              "aggregation_type": "max",
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100,
              "unknown_object": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            TimeSeriesStreamingAggregationBuilder result = TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser);

            assertEquals(StreamingAggregationType.MAX, result.getAggregationType());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(100L, result.getStep());
            assertNull("groupByTags should be null when not specified", result.getGroupByTags());
        }
    }

    public void testParseWithEmptyGroupByTags() throws Exception {
        String json = """
            {
              "aggregation_type": "sum",
              "group_by_tags": [],
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            TimeSeriesStreamingAggregationBuilder result = TimeSeriesStreamingAggregationBuilder.parse("test_agg", parser);

            assertNull("Empty group_by_tags array should result in null", result.getGroupByTags());
        }
    }

    // ---- Equals tests ----

    public void testEquals() {
        List<String> tags = List.of("host");
        TimeSeriesStreamingAggregationBuilder builder1 = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.SUM,
            tags,
            1000L,
            2000L,
            100L
        );
        TimeSeriesStreamingAggregationBuilder builder2 = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.SUM,
            tags,
            1000L,
            2000L,
            100L
        );
        assertTrue("Equal objects should return true", builder1.equals(builder2));

        // Different aggregationType
        TimeSeriesStreamingAggregationBuilder diffType = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.MIN,
            tags,
            1000L,
            2000L,
            100L
        );
        assertFalse("Different aggregationType should return false", builder1.equals(diffType));

        // Different groupByTags
        TimeSeriesStreamingAggregationBuilder diffTags = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.SUM,
            List.of("region"),
            1000L,
            2000L,
            100L
        );
        assertFalse("Different groupByTags should return false", builder1.equals(diffTags));

        // Different minTimestamp
        TimeSeriesStreamingAggregationBuilder diffMin = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.SUM,
            tags,
            999L,
            2000L,
            100L
        );
        assertFalse("Different minTimestamp should return false", builder1.equals(diffMin));

        // Different maxTimestamp
        TimeSeriesStreamingAggregationBuilder diffMax = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.SUM,
            tags,
            1000L,
            2001L,
            100L
        );
        assertFalse("Different maxTimestamp should return false", builder1.equals(diffMax));

        // Different step
        TimeSeriesStreamingAggregationBuilder diffStep = new TimeSeriesStreamingAggregationBuilder(
            "test",
            StreamingAggregationType.SUM,
            tags,
            1000L,
            2000L,
            200L
        );
        assertFalse("Different step should return false", builder1.equals(diffStep));

        // Different name
        TimeSeriesStreamingAggregationBuilder diffName = new TimeSeriesStreamingAggregationBuilder(
            "different",
            StreamingAggregationType.SUM,
            tags,
            1000L,
            2000L,
            100L
        );
        assertFalse("Different name should return false", builder1.equals(diffName));
    }

    // ---- registerAggregators test ----

    public void testRegisterAggregators() {
        ValuesSourceRegistry.Builder mockBuilder = mock(ValuesSourceRegistry.Builder.class);
        TimeSeriesStreamingAggregationBuilder.registerAggregators(mockBuilder);
    }

    // ---- Helper methods ----

    private QueryShardContext mockQueryShardContextWithTsdbEngine(Boolean tsdbEngineEnabled) {
        QueryShardContext mockContext = mock(QueryShardContext.class);

        Settings.Builder settingsBuilder = Settings.builder();
        if (tsdbEngineEnabled != null) {
            settingsBuilder.put("index.tsdb_engine.enabled", tsdbEngineEnabled);
        }
        Settings settings = settingsBuilder.build();

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        when(mockContext.getIndexSettings()).thenReturn(indexSettings);

        return mockContext;
    }
}
