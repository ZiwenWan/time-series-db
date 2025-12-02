/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;

/**
 * A Weight wrapper that prunes segments based on time range metadata.
 * <p>
 * This weight intercepts the scorer() call and returns null for segments whose
 * min/max timestamp range does not overlap with the query's time range. This
 * effectively skips ALL query evaluation (term filters, range filters, etc.)
 * for those segments at the Lucene level.
 * <p>
 * Benefits:
 * <ul>
 *   <li>Skips term/label filter evaluation on non-overlapping segments</li>
 *   <li>Skips range filter evaluation on non-overlapping segments</li>
 *   <li>Reduces CPU and I/O by avoiding unnecessary document iteration</li>
 *   <li>Works transparently with any query type</li>
 * </ul>
 */
public class TimeRangePruningWeight extends Weight {
    private final Weight delegate;
    private final long minTimestamp;
    private final long maxTimestamp;

    /**
     * Creates a new TimeRangePruningWeight.
     *
     * @param delegate the wrapped Weight to delegate to for overlapping segments
     * @param minTimestamp minimum timestamp of the query range (inclusive)
     * @param maxTimestamp maximum timestamp of the query range (exclusive)
     */
    protected TimeRangePruningWeight(Weight delegate, long minTimestamp, long maxTimestamp) {
        super(delegate.getQuery());
        this.delegate = delegate;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return delegate.explain(context, doc);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
        return delegate.matches(context, doc);
    }

    @Override
    public int count(LeafReaderContext context) throws IOException {
        // Extract TSDBLeafReader from the context
        TSDBLeafReader tsdbReader = TSDBLeafReader.unwrapLeafReader(context.reader());

        // Check if segment overlaps with query time range
        if (tsdbReader != null && !tsdbReader.overlapsTimeRange(minTimestamp, maxTimestamp)) {
            // Segment is outside the time range - return 0 as nothing can match
            return 0;
        }

        return super.count(context);
    }

    /**
     * Returns a ScorerSupplier for the given segment, or null if the segment's time range
     * does not overlap with the query's time range.
     * <p>
     * This is where segment pruning happens - by returning null, we signal to Lucene
     * that there are no matching documents in this segment, which skips all query
     * evaluation including term filters and range filters.
     * <p>
     * Note: We override scorerSupplier() rather than scorer() because scorer() is final
     * in Lucene's Weight class. The scorer() method internally calls scorerSupplier(),
     * so our pruning logic still applies.
     *
     * @param context the segment to score
     * @return a ScorerSupplier if the segment overlaps the time range, null otherwise
     * @throws IOException if an error occurs
     */
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        // Extract TSDBLeafReader from the context
        TSDBLeafReader tsdbReader = TSDBLeafReader.unwrapLeafReader(context.reader());

        // Check if segment overlaps with query time range
        if (tsdbReader != null && !tsdbReader.overlapsTimeRange(minTimestamp, maxTimestamp)) {
            // Segment is outside the time range - return null to skip all query evaluation
            return null;
        }

        // Segment overlaps - delegate to the wrapped weight
        return delegate.scorerSupplier(context);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return delegate.isCacheable(ctx);
    }
}
