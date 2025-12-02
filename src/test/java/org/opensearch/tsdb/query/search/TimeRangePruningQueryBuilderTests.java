/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class TimeRangePruningQueryBuilderTests extends AbstractQueryTestCase<TimeRangePruningQueryBuilder> {

    @Override
    protected TimeRangePruningQueryBuilder doCreateTestQueryBuilder() {
        return new TimeRangePruningQueryBuilder(
            new MatchAllQueryBuilder(), // delegate
            randomLongBetween(0, 10000L),
            randomLongBetween(20000L, 30000L)
        );
    }

    @Override
    protected void doAssertLuceneQuery(
        TimeRangePruningQueryBuilder timeRangePruningQueryBuilder,
        Query query,
        QueryShardContext queryShardContext
    ) throws IOException {
        assertNotNull(query);
        assertTrue("Query should be a TimeRangePruningQuery", query instanceof TimeRangePruningQuery);

        TimeRangePruningQuery pruningQuery = (TimeRangePruningQuery) query;
        assertEquals(timeRangePruningQueryBuilder.getMinTimestamp(), pruningQuery.getMinTimestamp());
        assertEquals(timeRangePruningQueryBuilder.getMaxTimestamp(), pruningQuery.getMaxTimestamp());
        assertNotNull(pruningQuery.getDelegate());
    }

    // also register the delegate
    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(TimeRangePruningQueryBuilder.NAME),
                    TimeRangePruningQueryBuilder::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    MatchAllQueryBuilder::fromXContent
                )
            )
        );
    }

    // also register the delegate
    @Override
    protected NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TimeRangePruningQueryBuilder.NAME, TimeRangePruningQueryBuilder::new),
                new NamedWriteableRegistry.Entry(
                    QueryBuilder.class,
                    MatchAllQueryBuilder.NAME,
                    MatchAllQueryBuilder::new

                )
            )
        );
    }

    public void testConstructor() {
        QueryBuilder delegate = new MatchAllQueryBuilder();
        long minTimestamp = 1000L;
        long maxTimestamp = 2000L;

        TimeRangePruningQueryBuilder builder = new TimeRangePruningQueryBuilder(delegate, minTimestamp, maxTimestamp);

        assertEquals(delegate, builder.getDelegate());
        assertEquals(minTimestamp, builder.getMinTimestamp());
        assertEquals(maxTimestamp, builder.getMaxTimestamp());
    }

    public void testConstructorNullDelegate() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeRangePruningQueryBuilder(null, 1000L, 2000L)
        );
        assertEquals("Delegate query cannot be null", e.getMessage());
    }

    public void testConstructorInvalidTimeRange() {
        QueryBuilder delegate = new MatchAllQueryBuilder();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeRangePruningQueryBuilder(delegate, 2000L, 1000L)
        );
        assertTrue(e.getMessage().contains("minTimestamp"));
        assertTrue(e.getMessage().contains("must be less than maxTimestamp"));
    }

    public void testConstructorEqualTimestamps() {
        QueryBuilder delegate = new MatchAllQueryBuilder();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeRangePruningQueryBuilder(delegate, 1000L, 1000L)
        );
        assertTrue(e.getMessage().contains("minTimestamp"));
        assertTrue(e.getMessage().contains("must be less than maxTimestamp"));
    }

    public void testXContentGeneration() throws IOException {
        QueryBuilder delegate = QueryBuilders.termQuery("field", "value");
        TimeRangePruningQueryBuilder original = new TimeRangePruningQueryBuilder(delegate, 1000L, 2000L);

        try (XContentBuilder builder = jsonBuilder()) {
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = builder.toString();

            assertTrue(json.contains("time_range_pruner"));
            assertTrue(json.contains("min_timestamp"));
            assertTrue(json.contains("max_timestamp"));
            assertTrue(json.contains("1000"));
            assertTrue(json.contains("2000"));
        }
    }

    public void testEquals() {
        QueryBuilder delegate1 = new MatchAllQueryBuilder();
        QueryBuilder delegate2 = new TermQueryBuilder("field", "value");

        TimeRangePruningQueryBuilder builder1 = new TimeRangePruningQueryBuilder(delegate1, 1000L, 2000L);
        TimeRangePruningQueryBuilder builder2 = new TimeRangePruningQueryBuilder(delegate1, 1000L, 2000L);
        TimeRangePruningQueryBuilder builder3 = new TimeRangePruningQueryBuilder(delegate2, 1000L, 2000L);
        TimeRangePruningQueryBuilder builder4 = new TimeRangePruningQueryBuilder(delegate1, 1500L, 2000L);
        TimeRangePruningQueryBuilder builder5 = new TimeRangePruningQueryBuilder(delegate1, 1000L, 2500L);

        assertEquals(builder1, builder2);
        assertNotEquals(builder1, builder3);
        assertNotEquals(builder1, builder4);
        assertNotEquals(builder1, builder5);
    }

    public void testHashCode() {
        QueryBuilder delegate = new MatchAllQueryBuilder();
        TimeRangePruningQueryBuilder builder1 = new TimeRangePruningQueryBuilder(delegate, 1000L, 2000L);
        TimeRangePruningQueryBuilder builder2 = new TimeRangePruningQueryBuilder(delegate, 1000L, 2000L);

        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testGetWriteableName() {
        QueryBuilder delegate = new MatchAllQueryBuilder();
        TimeRangePruningQueryBuilder builder = new TimeRangePruningQueryBuilder(delegate, 1000L, 2000L);

        assertEquals("time_range_pruner", builder.getWriteableName());
        assertEquals("time_range_pruner", builder.getName());
    }
}
