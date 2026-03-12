/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.aggregator.AggregationExecStats;
import org.opensearch.tsdb.query.aggregator.AggregationDataSource;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;
import org.opensearch.tsdb.query.utils.ProfileInfoMapper;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.tsdb.metrics.TSDBMetricsConstants.NANOS_PER_MILLI;

/**
 * Transforms OpenSearch search responses into Prometheus-compatible matrix format.
 *
 * <p>This listener processes {@link SearchResponse} objects containing time series aggregations
 * and converts them into a matrix-formatted JSON response following the Prometheus query_range API structure.
 * The matrix format is particularly useful for representing time series data with multiple data points
 * over time.</p>
 *
 * <h2>Response Structure:</h2>
 * <pre>{@code
 * {
 *   "status": "success",
 *   "data": {
 *     "resultType": "matrix",
 *     "result": [
 *       {
 *         "metric": {
 *           "label1": "value1",
 *           "label2": "value2"
 *         },
 *         "alias": "metric_name",
 *         "values": [
 *           [timestamp1, "value1"],
 *           [timestamp2, "value2"]
 *         ],
 *         "step": 10000,
 *         "start": 1000000,
 *         "end": 2000000
 *       }
 *     ],
 *     "execStats" : {
 *        "latencyMs": 350,
 *         "data" : {
 *            "series": { "numInput": 9, "numOutput: 5},
 *            "samples" {"numInput": 240, "numOutput: 180 }
 *         },
 *        "storage": {
 *            "chunks": { "closed" : 120, "live": 12},
 *            "documents": { "closed" : 12000, "live": 12}
 *        },
 *        "resource": { "memoryBytes": 12089 }
 *     },
 *     "dataSource" : {
 *      "origin" : ["prometheus"],
 *      "indexes": [{ "index": "2d", "stepSize": "10s"}]
 *     }
 *   }
 * }
 * }</pre>
 *
 * <h2>Error Handling:</h2>
 * <p>If an exception occurs during transformation, the listener returns a 500 error with an error response:</p>
 * <pre>{@code
 * {
 *   "status": "error",
 *   "error": "error message"
 * }
 * }</pre>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * RestChannel channel = ...;
 * String aggregationName = "timeseries_agg";
 * PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, aggregationName);
 * client.search(request, listener);
 * }</pre>
 *
 * @see RestToXContentListener
 * @see TimeSeries
 */
public class PromMatrixResponseListener extends RestToXContentListener<SearchResponse> {

    // Response field names
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_RESULT_TYPE = "resultType";
    private static final String FIELD_RESULT = "result";
    private static final String FIELD_ERROR = "error";
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_VALUES = "values";

    // Response status values
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_ERROR = "error";

    // Response type values
    private static final String RESULT_TYPE_MATRIX = "matrix";

    // Exec stats field names
    private static final String FIELD_EXEC_STATS = "execStats";
    private static final String FIELD_LATENCY_MS = "latencyMs";
    private static final String FIELD_SERIES = "series";
    private static final String FIELD_SAMPLES = "samples";
    private static final String FIELD_NUM_INPUT = "numInput";
    private static final String FIELD_NUM_OUTPUT = "numOutput";
    private static final String FIELD_STORAGE = "storage";
    private static final String FIELD_CHUNKS = "chunks";
    private static final String FIELD_DOCUMENTS = "documents";
    private static final String FIELD_CLOSED = "closed";
    private static final String FIELD_LIVE = "live";
    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_MEMORY_BYTES = "memoryBytes";

    // Data source field names
    private static final String FIELD_DATA_SOURCE = "dataSource";
    private static final String FIELD_ORIGIN = "origin";
    private static final String FIELD_INDEXES = "indexes";
    private static final String FIELD_INDEX = "index";
    private static final String FIELD_STEP_SIZE = "stepSize";

    // Aggregator name for profile extraction
    private static final String TIME_SERIES_UNFOLD_AGGREGATOR_NAME = TimeSeriesUnfoldAggregator.class.getSimpleName();

    private final String finalAggregationName;

    private final boolean profile;

    private final boolean includeMetadata;

    private final boolean includeAlias;

    private final long startTimeNanos;

    private final QueryMetrics queryMetrics;

    private final boolean includeExecStats;

    private final boolean includeDataSource;

    /**
     * Container for query execution metrics.
     * Allows adding new metrics without changing constructor signatures.
     *
     * @param executionLatency histogram for overall query execution time (can be null)
     * @param collectPhaseLatencyMax histogram for max collect phase latency across shards (can be null)
     * @param reducePhaseLatencyMax histogram for max reduce phase latency across shards (can be null)
     * @param postCollectionPhaseLatencyMax histogram for max post collection phase latency across shards (can be null)
     * @param collectPhaseCpuTimeMs histogram for total collect CPU time across all shards in milliseconds (can be null)
     * @param reducePhaseCpuTimeMs histogram for total reduce CPU time across all shards in milliseconds (can be null)
     * @param shardLatencyMax histogram for max total shard processing time (can be null)
     */
    public record QueryMetrics(Histogram executionLatency, Histogram collectPhaseLatencyMax, Histogram reducePhaseLatencyMax,
        Histogram postCollectionPhaseLatencyMax, Histogram collectPhaseCpuTimeMs, Histogram reducePhaseCpuTimeMs,
        Histogram shardLatencyMax) {
    }

    /**
     * Creates a new matrix response listener.
     *
     * @param channel the REST channel to send the response to
     * @param finalAggregationName the name of the final aggregation to extract (must not be null)
     * @param profile whether to include profiling information in the response
     * @param includeMetadata whether to include metadata fields (step, start, end) in each time series
     * @param includeAlias whether to include the alias field in each time series
     * @throws NullPointerException if finalAggregationName is null
     */
    public PromMatrixResponseListener(
        RestChannel channel,
        String finalAggregationName,
        boolean profile,
        boolean includeMetadata,
        boolean includeAlias
    ) {
        this(channel, finalAggregationName, profile, includeMetadata, false, false, includeAlias, null);
    }

    /**
     * Creates a new matrix response listener with metrics recording capability.
     *
     * @param channel the REST channel to send the response to
     * @param finalAggregationName the name of the final aggregation to extract (must not be null)
     * @param profile whether to include profiling information in the response
     * @param includeMetadata whether to include metadata fields (step, start, end) in each time series
     * @param includeExecStats whether to include execution stats in the response
     * @param includeDataSource whether to include data source metadata in the response
     * @param includeAlias whether to include the alias field in each time series
     * @param queryMetrics container for query execution metrics (can be null)
     * @throws NullPointerException if finalAggregationName is null
     */
    public PromMatrixResponseListener(
        RestChannel channel,
        String finalAggregationName,
        boolean profile,
        boolean includeMetadata,
        boolean includeExecStats,
        boolean includeDataSource,
        boolean includeAlias,
        QueryMetrics queryMetrics
    ) {
        super(channel);
        this.finalAggregationName = Objects.requireNonNull(finalAggregationName, "finalAggregationName cannot be null");
        this.profile = profile;
        this.includeMetadata = includeMetadata;
        this.includeAlias = includeAlias;
        this.startTimeNanos = System.nanoTime();
        this.queryMetrics = queryMetrics;
        this.includeExecStats = includeExecStats;
        this.includeDataSource = includeDataSource;
    }

    /**
     * Returns whether execution stats are included in the response.
     *
     * @return true if execution stats are included, false otherwise
     */
    boolean isIncludeExecStats() {
        return includeExecStats;
    }

    /**
     * Returns whether data source metadata is included in the response.
     *
     * @return true if data source metadata is included, false otherwise
     */
    boolean isIncludeDataSource() {
        return includeDataSource;
    }

    /**
     * Builds the REST response from a search response by transforming it into matrix format.
     *
     * <p>This method is called by the framework when a successful search response is received.
     * It transforms the response into matrix format and returns a {@link BytesRestResponse}.</p>
     *
     * @param response the search response to transform
     * @param builder the XContent builder to use for constructing the response
     * @return a REST response with the transformed data
     * @throws Exception if an error occurs during transformation
     */
    @Override
    public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
        try {
            // Record metrics from the search response
            recordMetrics(response);

            transformToMatrixResponse(response, builder);
            return new BytesRestResponse(RestStatus.OK, builder);
        } catch (Exception e) {
            // Create a new builder for error response since the original builder may be in an invalid state
            XContentBuilder errorBuilder = channel.newErrorBuilder();
            return buildErrorResponse(errorBuilder, e);
        }
    }

    /**
     * Transforms a search response into matrix format and writes it to the XContent builder.
     *
     * <p>This method extracts time series data from the search response aggregations and transforms
     * it into Prometheus matrix format. If the includeMetadata flag is set to true, all time series in
     * the response will include their metadata: step size (query resolution) in milliseconds, start time,
     * and end time. Note that different time series may have different metadata values, but the
     * includeMetadata flag applies to all time series in the response.</p>
     *
     * @param response the search response containing time series aggregations
     * @param builder the XContent builder to write the matrix response to
     * @throws IOException if an I/O error occurs during writing
     */
    private void transformToMatrixResponse(SearchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(FIELD_STATUS, STATUS_SUCCESS);
        builder.startObject(FIELD_DATA);
        builder.field(FIELD_RESULT_TYPE, RESULT_TYPE_MATRIX);
        builder.field(
            FIELD_RESULT,
            TimeSeriesOutputMapper.extractAndTransformToPromMatrix(
                response.getAggregations(),
                finalAggregationName,
                includeMetadata,
                includeAlias
            )
        );
        builder.endObject();

        // Add profiling information if requested
        if (profile) {
            ProfileInfoMapper.extractProfileInfo(response, builder);
        }

        // Add execution stats if requested and available (stats are EMPTY when serialFormatSetting < 2)
        if (includeExecStats || includeDataSource) {
            InternalTimeSeries its = findInternalTimeSeries(response.getAggregations(), finalAggregationName);

            if (includeExecStats && its != null && !AggregationExecStats.EMPTY.equals(its.getExecStats())) {
                AggregationExecStats stats = its.getExecStats();
                double latencyMs = (System.nanoTime() - startTimeNanos) / NANOS_PER_MILLI;
                long numSeriesOutput = its.getTimeSeries().size();
                long numSamplesOutput = its.getTimeSeries().stream().mapToLong(ts -> ts.getSamples().size()).sum();

                builder.startObject(FIELD_EXEC_STATS);
                builder.field(FIELD_LATENCY_MS, latencyMs);
                builder.startObject(FIELD_DATA);
                builder.startObject(FIELD_SERIES);
                builder.field(FIELD_NUM_INPUT, stats.seriesNumInput());
                builder.field(FIELD_NUM_OUTPUT, numSeriesOutput);
                builder.endObject();
                builder.startObject(FIELD_SAMPLES);
                builder.field(FIELD_NUM_INPUT, stats.samplesNumInput());
                builder.field(FIELD_NUM_OUTPUT, numSamplesOutput);
                builder.endObject();
                builder.endObject();
                builder.startObject(FIELD_STORAGE);
                builder.startObject(FIELD_CHUNKS);
                builder.field(FIELD_CLOSED, stats.chunksNumClosed());
                builder.field(FIELD_LIVE, stats.chunksNumLive());
                builder.endObject();
                builder.startObject(FIELD_DOCUMENTS);
                builder.field(FIELD_CLOSED, stats.docsNumClosed());
                builder.field(FIELD_LIVE, stats.docsNumLive());
                builder.endObject();
                builder.endObject();
                builder.startObject(FIELD_RESOURCE);
                builder.field(FIELD_MEMORY_BYTES, stats.memoryBytes());
                builder.endObject();
                builder.endObject(); // execStats
            }

            if (includeDataSource && its != null && !AggregationDataSource.EMPTY.equals(its.getDataSource())) {
                AggregationDataSource ds = its.getDataSource();
                builder.startObject(FIELD_DATA_SOURCE);
                builder.array(FIELD_ORIGIN, ds.origins().toArray(new String[0]));
                builder.startArray(FIELD_INDEXES);
                for (AggregationDataSource.IndexInfo idx : ds.indexes()) {
                    builder.startObject();
                    builder.field(FIELD_INDEX, idx.index());
                    builder.field(FIELD_STEP_SIZE, idx.stepSize());
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
        }

        builder.endObject();
    }

    /**
     * Finds the first {@link InternalTimeSeries} in the aggregations that matches the given name.
     *
     * @param aggregations the aggregations to search (may be null)
     * @param aggName the aggregation name to match, or null to match any
     * @return the first matching {@link InternalTimeSeries}, or null if none found
     */
    private InternalTimeSeries findInternalTimeSeries(Aggregations aggregations, String aggName) {
        if (aggregations == null) return null;
        for (Aggregation agg : aggregations) {
            if (agg instanceof InternalTimeSeries its) {
                if (aggName == null || aggName.equals(its.getName())) {
                    return its;
                }
            }
        }
        return null;
    }

    /**
     * Builds an error response when transformation fails.
     *
     * @param builder the XContent builder to use for the error response
     * @param error the exception that caused the error
     * @return a REST response with error details
     * @throws IOException if an I/O error occurs during writing
     */
    private RestResponse buildErrorResponse(XContentBuilder builder, Exception error) throws IOException {
        builder.startObject();
        builder.field(FIELD_STATUS, STATUS_ERROR);
        builder.field(FIELD_ERROR, error.getMessage());
        builder.endObject();
        return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
    }

    /**
     * Records query execution metrics from the search response.
     *
     * <p>This method extracts and records:
     * <ul>
     *   <li><b>Overall query execution latency</b> - Always recorded (end-to-end time at REST layer)</li>
     *   <li><b>Shard latency MAX</b> - Max total shard processing time (collect + reduce on slowest shard, profiling required)</li>
     *   <li><b>Collect/Reduce/Post collection phase latency MAX</b> - Slowest shard (user-perceived latency, profiling required)</li>
     *   <li><b>Collect/Reduce phase work TOTAL</b> - Sum across all shards (total CPU work, profiling required)</li>
     * </ul>
     *
     * <p><b>Note:</b> Phase metrics require OpenSearch profiling to be enabled (profile=true parameter),
     * which adds overhead. The overall execution latency is always available as it's measured at
     * the REST action layer without requiring profiling.
     *
     * <p><b>MAX vs SUM:</b> Shards process queries in parallel, so MAX represents the actual user-perceived
     * latency (slowest shard), while SUM represents total CPU work across the cluster (useful for capacity planning).
     *
     * <p><b>Multiple Aggregators:</b> If multiple TimeSeriesUnfoldAggregator instances exist on a single shard
     * (unlikely in typical usage), their times are summed for that shard before comparing with the global max,
     * since aggregators execute sequentially on each shard.
     *
     * @param response the search response containing timing information
     */
    private void recordMetrics(SearchResponse response) {
        if (!TSDBMetrics.isInitialized() || queryMetrics == null) {
            return;
        }

        try {
            // Record overall execution latency (convert nanos to millis)
            if (queryMetrics.executionLatency != null) {
                long executionTimeNanos = System.nanoTime() - startTimeNanos;
                double executionTimeMillis = executionTimeNanos / NANOS_PER_MILLI;
                TSDBMetrics.recordHistogram(queryMetrics.executionLatency, executionTimeMillis);
            }

            // Extract and record collect and reduce phase metrics from profile results
            boolean needsPhaseMetrics = queryMetrics.collectPhaseLatencyMax != null
                || queryMetrics.reducePhaseLatencyMax != null
                || queryMetrics.postCollectionPhaseLatencyMax != null
                || queryMetrics.collectPhaseCpuTimeMs != null
                || queryMetrics.reducePhaseCpuTimeMs != null
                || queryMetrics.shardLatencyMax != null;

            if (needsPhaseMetrics && response.getProfileResults() != null && !response.getProfileResults().isEmpty()) {
                double maxCollectTimeMillis = 0.0;
                double maxReduceTimeMillis = 0.0;
                double maxPostCollectionTimeMillis = 0.0;
                double totalCollectTimeMillis = 0.0;
                double totalReduceTimeMillis = 0.0;
                double maxShardTotalTimeMillis = 0.0;

                for (ProfileShardResult shardResult : response.getProfileResults().values()) {
                    // Accumulate times for all aggregators on this shard
                    double currentShardCollectTimeMillis = 0.0;
                    double currentShardReduceTimeMillis = 0.0;
                    double currentShardPostCollectionTimeMillis = 0.0;
                    // TODO: Consolidate it with ProfileInfoMapper.extractPerShardStats()
                    if (shardResult.getAggregationProfileResults() != null) {
                        for (ProfileResult profileResult : shardResult.getAggregationProfileResults().getProfileResults()) {
                            // Only extract timing for TimeSeriesUnfoldAggregator
                            if (TIME_SERIES_UNFOLD_AGGREGATOR_NAME.equals(profileResult.getQueryName())) {
                                Map<String, Long> breakdown = profileResult.getTimeBreakdown();
                                if (breakdown != null) {
                                    // Extract and convert to millis immediately
                                    double collectTimeMillis = breakdown.getOrDefault("collect", 0L) / NANOS_PER_MILLI;
                                    double reduceTimeMillis = breakdown.getOrDefault("reduce", 0L) / NANOS_PER_MILLI;
                                    double postCollectionTimeMillis = breakdown.getOrDefault("post_collection", 0L) / NANOS_PER_MILLI;

                                    // Accumulate times for this shard
                                    currentShardCollectTimeMillis += collectTimeMillis;
                                    currentShardReduceTimeMillis += reduceTimeMillis;
                                    currentShardPostCollectionTimeMillis += postCollectionTimeMillis;
                                }
                            }
                        }
                    }

                    // Track MAX across shards (user-perceived latency - slowest shard)
                    maxCollectTimeMillis = Math.max(maxCollectTimeMillis, currentShardCollectTimeMillis);
                    maxReduceTimeMillis = Math.max(maxReduceTimeMillis, currentShardReduceTimeMillis);
                    maxPostCollectionTimeMillis = Math.max(maxPostCollectionTimeMillis, currentShardPostCollectionTimeMillis);
                    maxShardTotalTimeMillis = Math.max(
                        maxShardTotalTimeMillis,
                        currentShardCollectTimeMillis + currentShardReduceTimeMillis
                    );

                    // Track SUM across all shards (total work across the cluster)
                    totalCollectTimeMillis += currentShardCollectTimeMillis;
                    totalReduceTimeMillis += currentShardReduceTimeMillis;
                }

                // Record shard latency MAX (collect + reduce on slowest shard)
                if (queryMetrics.shardLatencyMax != null && maxShardTotalTimeMillis > 0) {
                    TSDBMetrics.recordHistogram(queryMetrics.shardLatencyMax, maxShardTotalTimeMillis);
                }

                // Record collect phase latency MAX
                if (queryMetrics.collectPhaseLatencyMax != null && maxCollectTimeMillis > 0) {
                    TSDBMetrics.recordHistogram(queryMetrics.collectPhaseLatencyMax, maxCollectTimeMillis);
                }

                // Record reduce phase latency MAX
                if (queryMetrics.reducePhaseLatencyMax != null && maxReduceTimeMillis > 0) {
                    TSDBMetrics.recordHistogram(queryMetrics.reducePhaseLatencyMax, maxReduceTimeMillis);
                }

                // Record post collection phase latency MAX
                if (queryMetrics.postCollectionPhaseLatencyMax != null && maxPostCollectionTimeMillis > 0) {
                    TSDBMetrics.recordHistogram(queryMetrics.postCollectionPhaseLatencyMax, maxPostCollectionTimeMillis);
                }

                // Record collect phase CPU time (sum across all shards)
                if (queryMetrics.collectPhaseCpuTimeMs != null && totalCollectTimeMillis > 0) {
                    TSDBMetrics.recordHistogram(queryMetrics.collectPhaseCpuTimeMs, totalCollectTimeMillis);
                }

                // Record reduce phase CPU time (sum across all shards)
                if (queryMetrics.reducePhaseCpuTimeMs != null && totalReduceTimeMillis > 0) {
                    TSDBMetrics.recordHistogram(queryMetrics.reducePhaseCpuTimeMs, totalReduceTimeMillis);
                }
            }
        } catch (Exception e) {
            // Silently ignore metrics recording failures to avoid impacting query execution
        }
    }
}
