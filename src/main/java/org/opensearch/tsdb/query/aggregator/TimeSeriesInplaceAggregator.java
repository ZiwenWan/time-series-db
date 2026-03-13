/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.breaker.CircuitBreakerBatcher;

import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.MaxStage;
import org.opensearch.tsdb.lang.m3.stage.MinStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Inplace aggregator that processes "fetch | aggregation" queries without reconstructing full time series.
 *
 * <p><strong>EXPERIMENTAL:</strong> This feature is experimental and does not guarantee correctness
 * when there is duplicated data across segments. Any duplication across live indices and
 * closed-chunk indices (or any segments in general) will lead to double-counting in aggregation
 * results. Use with caution and only on datasets known to have no cross-segment duplicates.</p>
 *
 * <p>This aggregator optimizes simple fetch + aggregation queries by processing data in an inplace
 * fashion, avoiding the memory overhead of creating intermediate TimeSeries objects. It supports
 * sum, min, max, and avg aggregations with optional label-based grouping.</p>
 *
 * <h2>Key Optimizations:</h2>
 * <ul>
 *   <li><strong>Memory Efficiency:</strong> Direct array operations instead of HashMap lookups</li>
 *   <li><strong>Label Skipping:</strong> Skip label reading entirely for no-tag aggregations</li>
 *   <li><strong>Single Pass:</strong> No postCollection() phase required</li>
 *   <li><strong>NaN Handling:</strong> Use NaN to indicate missing data, no boolean arrays needed</li>
 * </ul>
 *
 * <h2>Eligible Query Patterns:</h2>
 * <ul>
 *   <li>{@code fetch service:api | sum} - Global sum without grouping</li>
 *   <li>{@code fetch service:api | sum region} - Sum grouped by region tag</li>
 *   <li>{@code fetch service:api | avg host} - Average grouped by host tag</li>
 *   <li>{@code fetch service:api | min}, {@code fetch service:api | max} - Min/Max aggregations</li>
 * </ul>
 *
 * <h2>Performance Benefits:</h2>
 * <ul>
 *   <li><strong>Memory:</strong> ~8 bytes per time point vs ~64 bytes per TimeSeries + samples</li>
 *   <li><strong>Speed:</strong> O(1) array access vs O(log n) HashMap lookups</li>
 *   <li><strong>CPU:</strong> Skip label processing for global aggregations (30-50% savings)</li>
 * </ul>
 *
 * @since 0.0.1
 */
public class TimeSeriesInplaceAggregator extends BucketsAggregator {

    private static final Logger logger = LogManager.getLogger(TimeSeriesInplaceAggregator.class);

    // Core aggregation configuration
    private final InplaceAggregationType aggregationType;
    private final List<String> groupByTags;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;
    private final int timeArraySize;

    // Inplace aggregation state per bucket
    private final Map<Long, InplaceAggregationState> bucketStates = new HashMap<>();

    // Circuit breaker tracking
    private long circuitBreakerBytes = 0;
    private long maxCircuitBreakerBytes = 0;

    /** Batches circuit breaker updates at {@link CircuitBreakerBatcher#BATCH_THRESHOLD_BYTES} (5 MB). */
    private final CircuitBreakerBatcher circuitBreakerBatcher;

    // Metrics tracking
    private int totalDocsProcessed = 0;
    private int liveDocsProcessed = 0;
    private int closedDocsProcessed = 0;

    /**
     * Create a time series inplace aggregator.
     *
     * @param name The name of the aggregator
     * @param factories The sub-aggregation factories
     * @param aggregationType The type of aggregation (sum, min, max, avg)
     * @param groupByTags The list of tag names for grouping (null for global aggregation)
     * @param context The search context
     * @param parent The parent aggregator
     * @param bucketCardinality The cardinality upper bound
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param step The step size for timestamp alignment
     * @param metadata The aggregation metadata
     * @throws IOException If an error occurs during initialization
     */
    public TimeSeriesInplaceAggregator(
        String name,
        AggregatorFactories factories,
        InplaceAggregationType aggregationType,
        List<String> groupByTags,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        long minTimestamp,
        long maxTimestamp,
        long step,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCardinality, metadata);

        this.aggregationType = aggregationType;
        this.groupByTags = groupByTags;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
        this.circuitBreakerBatcher = new CircuitBreakerBatcher(this::commitToCircuitBreaker);

        // Pre-calculate array size for efficient memory allocation
        this.timeArraySize = calculateTimeArraySize(minTimestamp, maxTimestamp, step);

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Created inplace aggregator: type={}, groupByTags={}, timeRange=[{}, {}], step={}, arraySize={}",
                aggregationType.getDisplayName(),
                groupByTags,
                minTimestamp,
                maxTimestamp,
                step,
                timeArraySize
            );
        }
    }

    /**
     * Calculate the required array size for the time range.
     */
    private static int calculateTimeArraySize(long minTimestamp, long maxTimestamp, long step) {
        if (maxTimestamp <= minTimestamp) {
            throw new IllegalArgumentException("maxTimestamp must be greater than minTimestamp");
        }
        return (int) ((maxTimestamp - 1 - minTimestamp) / step) + 1;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // TODO: Extract TSDBLeafReader unwrap + time range check to a shared base class
        // (see also TimeSeriesUnfoldAggregator.getLeafCollector())
        // Check if this leaf reader can be pruned based on time range
        TSDBLeafReader tsdbLeafReader = TSDBLeafReader.unwrapLeafReader(ctx.reader());
        if (tsdbLeafReader == null) {
            throw new IOException("Expected TSDBLeafReader but found: " + ctx.reader().getClass().getName());
        }
        if (!tsdbLeafReader.overlapsTimeRange(minTimestamp, maxTimestamp)) {
            // No matching data in this segment, skip it by returning the sub-collector
            return sub;
        }

        return new InplaceLeafBucketCollector(sub, ctx, tsdbLeafReader);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] bucketOrds) throws IOException {
        // Flush pending circuit breaker bytes before building results
        circuitBreakerBatcher.flush();

        InternalAggregation[] results = new InternalAggregation[bucketOrds.length];

        // Create the reduce stage that matches our inplace aggregation type,
        // so that cross-shard reduction properly re-aggregates partial results.
        UnaryPipelineStage reduceStage = createReduceStage();

        for (int i = 0; i < bucketOrds.length; i++) {
            long bucketOrd = bucketOrds[i];
            InplaceAggregationState state = bucketStates.get(bucketOrd);

            List<TimeSeries> timeSeries = state != null ? state.getFinalResults(minTimestamp, maxTimestamp, step) : Collections.emptyList();

            results[i] = new InternalTimeSeries(name, timeSeries, metadata(), reduceStage);
        }

        return results;
    }

    /**
     * Create the pipeline stage that corresponds to this inplace aggregation type.
     * This stage is used during cross-shard reduce to properly combine partial results.
     */
    private UnaryPipelineStage createReduceStage() {
        List<String> tags = (groupByTags != null) ? groupByTags : Collections.emptyList();
        switch (aggregationType) {
            case SUM:
                return tags.isEmpty() ? new SumStage() : new SumStage(tags);
            case MIN:
                return tags.isEmpty() ? new MinStage() : new MinStage(tags);
            case MAX:
                return tags.isEmpty() ? new MaxStage() : new MaxStage(tags);
            case AVG:
                return tags.isEmpty() ? new AvgStage() : new AvgStage(tags);
            default:
                throw new IllegalStateException("Unknown inplace aggregation type: " + aggregationType);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeries(name, Collections.emptyList(), metadata());
    }

    /**
     * Track memory allocation with batching via {@link CircuitBreakerBatcher}.
     */
    private void trackCircuitBreakerBytes(long bytes) {
        circuitBreakerBatcher.accept(bytes);
    }

    /**
     * Commit bytes directly to the parent's circuit breaker.
     * Called by {@link CircuitBreakerBatcher} when the batch threshold is exceeded.
     */
    private void commitToCircuitBreaker(long bytes) {
        if (bytes > 0) {
            try {
                addRequestCircuitBreakerBytes(bytes);
                circuitBreakerBytes += bytes;
                maxCircuitBreakerBytes = Math.max(maxCircuitBreakerBytes, circuitBreakerBytes);
            } catch (CircuitBreakingException e) {
                logger.warn(
                    "Circuit breaker tripped in inplace aggregator '{}': limit={} MiB, bytes={}, total={}, "
                        + "timeRange=[{}, {}], step={}, type={}",
                    name(),
                    String.format(Locale.ROOT, "%.2f", e.getByteLimit() / (1024.0 * 1024.0)),
                    bytes,
                    circuitBreakerBytes,
                    minTimestamp,
                    maxTimestamp,
                    step,
                    aggregationType.getDisplayName()
                );
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.circuitBreakerTrips, 1);
                throw e;
            }
        }
    }

    @Override
    public void doClose() {
        // Flush any remaining pending circuit breaker bytes
        circuitBreakerBatcher.flush();

        logger.debug(
            () -> new ParameterizedMessage(
                "Closing inplace aggregator '{}': total circuit breaker bytes tracked={}, peak={}",
                name(),
                RamUsageEstimator.humanReadableUnits(circuitBreakerBytes),
                RamUsageEstimator.humanReadableUnits(maxCircuitBreakerBytes)
            )
        );

        // Record peak circuit breaker usage metric
        if (maxCircuitBreakerBytes > 0) {
            TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.circuitBreakerMiB, maxCircuitBreakerBytes / (1024.0 * 1024.0));
        }
    }

    /**
     * Create inplace aggregation state for a bucket based on aggregation configuration.
     */
    private InplaceAggregationState createInplaceAggregationState() {
        if (groupByTags == null || groupByTags.isEmpty()) {
            // No-tag case: single time series result
            return new NoTagInplaceAggregationState(aggregationType, timeArraySize, minTimestamp, maxTimestamp, step);
        } else {
            // Tag-based case: grouped time series results
            return new TagInplaceAggregationState(aggregationType, groupByTags, timeArraySize, minTimestamp, maxTimestamp, step);
        }
    }

    /**
     * Leaf bucket collector that processes documents in inplace fashion.
     */
    private class InplaceLeafBucketCollector extends LeafBucketCollectorBase {

        private final LeafBucketCollector subCollector;
        private final TSDBLeafReader tsdbLeafReader;
        private final TSDBDocValues tsdbDocValues;

        public InplaceLeafBucketCollector(LeafBucketCollector sub, LeafReaderContext ctx, TSDBLeafReader tsdbLeafReader)
            throws IOException {
            super(sub, ctx);
            this.subCollector = sub;
            this.tsdbLeafReader = tsdbLeafReader;
            this.tsdbDocValues = tsdbLeafReader.getTSDBDocValues();
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            // Get or create inplace aggregation state for this bucket
            InplaceAggregationState state = bucketStates.computeIfAbsent(bucket, k -> createInplaceAggregationState());

            // Process document in inplace fashion
            state.processDocument(doc, tsdbDocValues, tsdbLeafReader);

            // Track memory usage for circuit breaker — incremental delta tracking
            long delta = state.consumeMemoryDelta();
            if (delta > 0) {
                trackCircuitBreakerBytes(delta);
            }

            // Track metrics
            boolean isLiveReader = tsdbLeafReader instanceof LiveSeriesIndexLeafReader;
            totalDocsProcessed++;
            if (isLiveReader) {
                liveDocsProcessed++;
            } else {
                closedDocsProcessed++;
            }

            // Call sub-collector
            collectBucket(subCollector, doc, bucket);
        }
    }
}
