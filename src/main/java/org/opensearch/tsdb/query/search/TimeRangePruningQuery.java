/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * A query wrapper that prunes segments based on time range metadata before
 * executing the wrapped query.
 * <p>
 * This query wraps any Lucene query and adds segment-level pruning based on
 * the min/max timestamp metadata stored in TSDBLeafReader instances. When a
 * segment's time range does not overlap with the query's time range, the
 * segment is completely skipped - no term filtering, no range filtering, no
 * document iteration.
 * <p>
 * Usage:
 * <pre>
 * Query originalQuery = new BooleanQuery.Builder()
 *     .add(new TermQuery(...), BooleanClause.Occur.FILTER)
 *     .add(new RangeQuery(...), BooleanClause.Occur.FILTER)
 *     .build();
 *
 * Query prunedQuery = new TimeRangePruningQuery(
 *     originalQuery,
 *     minTimestamp,  // query start time
 *     maxTimestamp   // query end time
 * );
 *
 * searcher.search(prunedQuery, collector);
 * </pre>
 * <p>
 * This is particularly effective for TSDB workloads where:
 * <ul>
 *   <li>Segments represent distinct time ranges (closed chunks)</li>
 *   <li>Queries often target specific time windows</li>
 *   <li>Label/term filters are expensive to evaluate</li>
 * </ul>
 */
public class TimeRangePruningQuery extends Query {
    private final Query delegate;
    private final long minTimestamp;
    private final long maxTimestamp;

    /**
     * Creates a new TimeRangePruningQuery.
     *
     * @param delegate the query to wrap and prune segments for
     * @param minTimestamp minimum timestamp of the query range (inclusive)
     * @param maxTimestamp maximum timestamp of the query range (exclusive)
     */
    public TimeRangePruningQuery(Query delegate, long minTimestamp, long maxTimestamp) {
        this.delegate = Objects.requireNonNull(delegate, "delegate query cannot be null");
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    /**
     * Returns the wrapped query.
     */
    public Query getDelegate() {
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

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        // Rewrite the delegate query first
        Query rewritten = delegate.rewrite(indexSearcher);

        // If the delegate was rewritten, wrap the rewritten version
        if (rewritten != delegate) {
            return new TimeRangePruningQuery(rewritten, minTimestamp, maxTimestamp);
        }

        // No rewriting needed
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        // Create the delegate weight first
        Weight delegateWeight = delegate.createWeight(searcher, scoreMode, boost);

        // Wrap it in our time-range pruning weight
        return new TimeRangePruningWeight(delegateWeight, minTimestamp, maxTimestamp);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegate.visit(visitor);
    }

    @Override
    public String toString(String field) {
        return "TimeRangePruningQuery("
            + "delegate="
            + delegate.toString(field)
            + ", minTimestamp="
            + minTimestamp
            + ", maxTimestamp="
            + maxTimestamp
            + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeRangePruningQuery that = (TimeRangePruningQuery) obj;
        return minTimestamp == that.minTimestamp && maxTimestamp == that.maxTimestamp && Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, minTimestamp, maxTimestamp);
    }
}
