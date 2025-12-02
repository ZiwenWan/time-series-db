/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A QueryBuilder that wraps another query and adds segment-level time-range pruning.
 * <p>
 * This query builder integrates with OpenSearch's query DSL and creates a
 * {@link TimeRangePruningQuery} at the Lucene level. This enables segment-level
 * pruning where segments whose min/max timestamp do not overlap with the query's
 * time range are completely skipped.
 * <p>
 * Usage in SourceBuilderVisitor:
 * <pre>{@code
 * private QueryBuilder buildQueryForFetch(FetchPlanNode planNode, TimeRange range) {
 *     BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
 *     // ... add term filters, range filters, etc ...
 *
 *     // Wrap with time-range pruning to skip segments outside time range
 *     return new TimeRangePruningQueryBuilder(boolQuery, range.start(), range.end());
 * }
 * }</pre>
 * <p>
 * Benefits:
 * <ul>
 *   <li>Skips term/label filtering on non-overlapping segments</li>
 *   <li>Skips range filter evaluation on non-overlapping segments</li>
 *   <li>Integrates cleanly with OpenSearch query DSL</li>
 *   <li>Works with query caching and rewriting</li>
 * </ul>
 */
public class TimeRangePruningQueryBuilder extends AbstractQueryBuilder<TimeRangePruningQueryBuilder> {

    public static final String NAME = "time_range_pruner";

    private final QueryBuilder delegate;
    private final long minTimestamp;
    private final long maxTimestamp;

    /**
     * Creates a new TimeRangePruningQueryBuilder.
     *
     * @param delegate the query to wrap
     * @param minTimestamp minimum timestamp of the query range (inclusive)
     * @param maxTimestamp maximum timestamp of the query range (exclusive)
     */
    public TimeRangePruningQueryBuilder(QueryBuilder delegate, long minTimestamp, long maxTimestamp) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate query cannot be null");
        }
        if (minTimestamp >= maxTimestamp) {
            throw new IllegalArgumentException(
                "Invalid time range: minTimestamp (" + minTimestamp + ") must be less than maxTimestamp (" + maxTimestamp + ")"
            );
        }
        this.delegate = delegate;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    /**
     * Read from stream.
     */
    public TimeRangePruningQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.delegate = in.readNamedWriteable(QueryBuilder.class);
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(delegate);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("min_timestamp", minTimestamp);
        builder.field("max_timestamp", maxTimestamp);
        builder.field("query");
        delegate.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Parse from XContent (for JSON queries).
     */
    public static TimeRangePruningQueryBuilder fromXContent(XContentParser parser) throws IOException {
        QueryBuilder delegate = null;
        long minTimestamp = 0;
        long maxTimestamp = 0;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("min_timestamp".equals(currentFieldName)) {
                    minTimestamp = parser.longValue();
                } else if ("max_timestamp".equals(currentFieldName)) {
                    maxTimestamp = parser.longValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    delegate = parseInnerQueryBuilder(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            }
        }

        if (delegate == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] requires a 'query' field");
        }

        TimeRangePruningQueryBuilder queryBuilder = new TimeRangePruningQueryBuilder(delegate, minTimestamp, maxTimestamp);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // Build the delegate Lucene query
        Query delegateQuery = delegate.toQuery(context);

        // Wrap it with time-range pruning
        return new TimeRangePruningQuery(delegateQuery, minTimestamp, maxTimestamp);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = delegate.rewrite(queryRewriteContext);
        if (rewritten != delegate) {
            // Delegate was rewritten - create new wrapper with rewritten query
            return new TimeRangePruningQueryBuilder(rewritten, minTimestamp, maxTimestamp);
        }
        return this;
    }

    @Override
    protected boolean doEquals(TimeRangePruningQueryBuilder other) {
        return Objects.equals(delegate, other.delegate) && minTimestamp == other.minTimestamp && maxTimestamp == other.maxTimestamp;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(delegate, minTimestamp, maxTimestamp);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Returns the wrapped query.
     */
    public QueryBuilder getDelegate() {
        return delegate;
    }

    /**
     * Returns the minimum timestamp (inclusive).
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Returns the maximum timestamp (exclusive).
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }
}
