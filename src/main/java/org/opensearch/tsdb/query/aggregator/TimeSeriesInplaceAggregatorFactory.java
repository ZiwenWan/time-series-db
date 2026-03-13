/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating time series inplace aggregator instances.
 *
 * <p>This factory creates {@link TimeSeriesInplaceAggregator} instances with the
 * specified configuration including aggregation type, grouping tags, time range,
 * and step size. It handles the creation and configuration of inplace aggregators
 * for optimized time series data processing.</p>
 *
 * <h2>Key Responsibilities:</h2>
 * <ul>
 *   <li><strong>Aggregator Creation:</strong> Creates properly configured
 *       {@link TimeSeriesInplaceAggregator} instances</li>
 *   <li><strong>Configuration Management:</strong> Manages aggregation type,
 *       grouping tags, time range, and step size configuration</li>
 *   <li><strong>Context Handling:</strong> Provides appropriate search context
 *       and parent aggregator relationships</li>
 *   <li><strong>CSS Support:</strong> Enables concurrent segment search for
 *       inplace aggregations (safe for all types)</li>
 * </ul>
 *
 * <h2>Inplace Optimizations:</h2>
 * <ul>
 *   <li><strong>Memory Efficiency:</strong> Direct array operations without
 *       intermediate TimeSeries object creation</li>
 *   <li><strong>Label Optimization:</strong> Conditional label reading based
 *       on grouping requirements</li>
 *   <li><strong>Single Pass:</strong> No post-collection phase required</li>
 * </ul>
 */
public class TimeSeriesInplaceAggregatorFactory extends AggregatorFactory {

    private final InplaceAggregationType aggregationType;
    private final List<String> groupByTags;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    /**
     * Create a time series inplace aggregator factory.
     *
     * @param name The name of the aggregator
     * @param queryShardContext The query shard context
     * @param parent The parent aggregator factory
     * @param subFactoriesBuilder The sub-aggregations builder
     * @param metadata The aggregation metadata
     * @param aggregationType The type of aggregation (sum, min, max, avg)
     * @param groupByTags The list of tag names for grouping (null for global aggregation)
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param step The step size for timestamp alignment
     * @throws IOException If an error occurs during initialization
     */
    public TimeSeriesInplaceAggregatorFactory(
        String name,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        InplaceAggregationType aggregationType,
        List<String> groupByTags,
        long minTimestamp,
        long maxTimestamp,
        long step
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.aggregationType = aggregationType;
        this.groupByTags = groupByTags;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new TimeSeriesInplaceAggregator(
            name,
            factories,
            aggregationType,
            groupByTags,
            searchContext,
            parent,
            cardinality,
            minTimestamp,
            maxTimestamp,
            step,
            metadata
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        // CSS-safe because TSDB's data model guarantees no duplicate samples across segments:
        // each time series sample exists in exactly one segment, so concurrent segment reads
        // produce disjoint inputs that can be safely merged via reduce.
        return true;
    }

    /**
     * Get the configured aggregation type.
     *
     * @return The inplace aggregation type
     */
    public InplaceAggregationType getAggregationType() {
        return aggregationType;
    }

    /**
     * Get the configured grouping tags.
     *
     * @return The list of tag names for grouping, or null for global aggregation
     */
    public List<String> getGroupByTags() {
        return groupByTags;
    }

    /**
     * Get the minimum timestamp for filtering.
     *
     * @return The minimum timestamp (inclusive)
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Get the maximum timestamp for filtering.
     *
     * @return The maximum timestamp (inclusive)
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Get the step size for timestamp alignment.
     *
     * @return The step size in milliseconds
     */
    public long getStep() {
        return step;
    }

    /**
     * Get estimated time array size for memory planning.
     *
     * @return The calculated array size based on time range and step
     */
    public int getEstimatedTimeArraySize() {
        return (int) ((maxTimestamp - 1 - minTimestamp) / step) + 1;
    }
}
