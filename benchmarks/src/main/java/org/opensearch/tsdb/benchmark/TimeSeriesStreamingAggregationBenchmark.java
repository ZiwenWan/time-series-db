/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.aggregator.StreamingAggregationType;
import org.opensearch.tsdb.query.aggregator.TimeSeriesStreamingAggregatorFactory;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing TimeSeriesUnfoldAggregator (with SumStage) vs TimeSeriesStreamingAggregator
 * for "fetch | sum" and "fetch | sum zone service" queries using production-like labels.
 *
 * <p>Each time series has 12 labels mimicking a production metric topology.
 * All tag values are selected from fixed pools for realistic cardinality distribution.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class TimeSeriesStreamingAggregationBenchmark extends BaseTSDBBenchmark {

    @Param({ "10000", "100000" })
    public int cardinality;

    @Param({ "100" })
    public int sampleCount;

    private Aggregator unfoldSumAggregator;
    private Aggregator streamingSumAggregator;
    private Aggregator unfoldSumByTagsAggregator;
    private Aggregator streamingSumByTagsAggregator;

    // All label values are selected from fixed pools
    private static final String[] REGIONS = { "dca", "phx" };
    private static final String[] ZONES = {
        "dca01", "dca02", "dca03", "dca04", "dca05", "dca06", "dca07", "dca08", "dca09", "dca10",
        "phx01", "phx02", "phx03", "phx04", "phx05", "phx06", "phx07", "phx08", "phx09", "phx10" };
    private static final String[] SERVICES = {
        "api-gateway", "user-service", "order-service", "payment-service", "notification-service", "auth-service" };
    private static final String[] HOSTS = {
        "dca-web001", "dca-web002", "dca-web003", "dca-web004", "dca-web005",
        "dca-app001", "dca-app002", "dca-app003", "dca-app004", "dca-app005",
        "phx-web001", "phx-web002", "phx-web003", "phx-web004", "phx-web005",
        "phx-app001", "phx-app002", "phx-app003", "phx-app004", "phx-app005" };
    private static final String[] CLUSTERS = { "primary", "secondary", "canary" };
    private static final String[] ENVS = { "production", "staging" };
    private static final String[] TEAMS = { "platform", "commerce", "identity", "infra" };
    private static final String[] VERSIONS = { "v1.2.3", "v1.2.4", "v1.3.0", "v2.0.0" };
    private static final String[] INSTANCES = {
        "dca-web001:8080", "dca-web002:8080", "dca-app001:8081", "dca-app002:8081",
        "phx-web001:8080", "phx-web002:8080", "phx-app001:8081", "phx-app002:8081",
        "dca-web003:8080", "dca-web004:8080", "phx-web003:8080", "phx-web004:8080" };
    private static final String[] PODS = {
        "api-gateway-6f8b9c-x4k2p", "api-gateway-6f8b9c-m9j3q", "user-service-a3d1e7-r7h5n",
        "user-service-a3d1e7-w2k8m", "order-service-b7c4f2-p3j6t", "order-service-b7c4f2-q8n1v",
        "payment-service-d9e5a1-s5m2x", "payment-service-d9e5a1-k4h7r", "notification-service-c2f8d3-y6p9w",
        "auth-service-e4g6b8-t1n3z", "auth-service-e4g6b8-u7j5q", "notification-service-c2f8d3-v8m4k" };
    private static final String[] NAMESPACES = { "default", "kube-system", "monitoring", "apps" };

    private static final List<String> GROUP_BY_TAGS = List.of("zone", "service");

    @Setup(Level.Trial)
    public void setup() throws IOException {
        setupBenchmark(this.cardinality, this.sampleCount, 12);
    }

    @Setup(Level.Invocation)
    public void setupAggregators() throws IOException {
        // --- Global sum (no tags) ---
        unfoldSumAggregator = createUnfoldAggregator("unfold_sum", new SumStage());
        streamingSumAggregator = createStreamingAggregator("streaming_sum", null);

        // --- Sum by zone, service ---
        unfoldSumByTagsAggregator = createUnfoldAggregator("unfold_sum_tags", new SumStage(GROUP_BY_TAGS));
        streamingSumByTagsAggregator = createStreamingAggregator("streaming_sum_tags", GROUP_BY_TAGS);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tearDownBenchmark();
    }

    // ==================== Global sum benchmarks ====================

    /**
     * Benchmark: Unfold aggregator with SumStage — "fetch | sum".
     */
    @Benchmark
    public void unfoldSum(Blackhole bh) throws IOException {
        bh.consume(runAggregation(unfoldSumAggregator));
    }

    /**
     * Benchmark: Streaming aggregator with SUM — "fetch | sum".
     */
    @Benchmark
    public void streamingSum(Blackhole bh) throws IOException {
        bh.consume(runAggregation(streamingSumAggregator));
    }

    // ==================== Sum by tags benchmarks ====================

    /**
     * Benchmark: Unfold aggregator with SumStage grouped by zone+service — "fetch | sum zone service".
     */
    @Benchmark
    public void unfoldSumByTags(Blackhole bh) throws IOException {
        bh.consume(runAggregation(unfoldSumByTagsAggregator));
    }

    /**
     * Benchmark: Streaming aggregator with SUM grouped by zone+service — "fetch | sum zone service".
     */
    @Benchmark
    public void streamingSumByTags(Blackhole bh) throws IOException {
        bh.consume(runAggregation(streamingSumByTagsAggregator));
    }

    // ==================== Helpers ====================

    private Aggregator createUnfoldAggregator(String name, SumStage stage) throws IOException {
        TimeSeriesUnfoldAggregatorFactory factory = new TimeSeriesUnfoldAggregatorFactory(
            name,
            searchContext.getQueryShardContext(),
            null,
            AggregatorFactories.builder(),
            Collections.emptyMap(),
            List.of(stage),
            MIN_TS,
            maxTs,
            STEP
        );
        return factory.createInternal(searchContext, null, CardinalityUpperBound.ONE, Collections.emptyMap());
    }

    private Aggregator createStreamingAggregator(String name, List<String> groupByTags) throws IOException {
        TimeSeriesStreamingAggregatorFactory factory = new TimeSeriesStreamingAggregatorFactory(
            name,
            searchContext.getQueryShardContext(),
            null,
            AggregatorFactories.builder(),
            Collections.emptyMap(),
            StreamingAggregationType.SUM,
            groupByTags,
            MIN_TS,
            maxTs,
            STEP
        );
        return factory.createInternal(searchContext, null, CardinalityUpperBound.ONE, Collections.emptyMap());
    }

    private InternalAggregation runAggregation(Aggregator aggregator) throws IOException {
        aggregator.preCollection();
        indexSearcher.search(rewritten, aggregator);
        aggregator.postCollection();
        return finalizeReduction(aggregator);
    }

    /**
     * Override data generation to produce production-like labels.
     * All tag values are selected from fixed pools — no randomly generated strings.
     */
    @Override
    protected void indexTimeSeries(ClosedChunkIndex index, int cardinality, int sampleCount, int labelCount) throws IOException {
        Random rng = new Random(42); // fixed seed for reproducibility

        for (int i = 0; i < cardinality; i++) {
            Map<String, String> labelsMap = new HashMap<>();
            labelsMap.put("__name__", "http_request_duration_seconds");
            labelsMap.put("region", REGIONS[rng.nextInt(REGIONS.length)]);
            labelsMap.put("zone", ZONES[rng.nextInt(ZONES.length)]);
            labelsMap.put("service", SERVICES[rng.nextInt(SERVICES.length)]);
            labelsMap.put("host", HOSTS[rng.nextInt(HOSTS.length)]);
            labelsMap.put("cluster", CLUSTERS[rng.nextInt(CLUSTERS.length)]);
            labelsMap.put("env", ENVS[rng.nextInt(ENVS.length)]);
            labelsMap.put("team", TEAMS[rng.nextInt(TEAMS.length)]);
            labelsMap.put("version", VERSIONS[rng.nextInt(VERSIONS.length)]);
            labelsMap.put("instance", INSTANCES[rng.nextInt(INSTANCES.length)]);
            labelsMap.put("pod", PODS[rng.nextInt(PODS.length)]);
            labelsMap.put("namespace", NAMESPACES[rng.nextInt(NAMESPACES.length)]);

            Labels labels = ByteLabels.fromMap(labelsMap);

            List<FloatSample> samples = new ArrayList<>(sampleCount);
            long minTimestamp = TIMESTAMP_MULTIPLIER;
            long maxTimestamp = TIMESTAMP_MULTIPLIER * sampleCount;

            for (int j = 1; j <= sampleCount; j++) {
                long timestamp = TIMESTAMP_MULTIPLIER * j;
                double value = 50.0 + rng.nextDouble() * 450.0;
                samples.add(new FloatSample(timestamp, (float) value));
            }

            MemChunk memChunk = new MemChunk(samples.size(), minTimestamp, maxTimestamp, null, Encoding.XOR);
            for (FloatSample sample : samples) {
                memChunk.append(sample.getTimestamp(), sample.getValue(), 0L);
            }

            index.addNewChunk(labels, memChunk);
        }
    }

    private InternalAggregation.ReduceContext createReduceContext(Aggregator aggregator) {
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        return InternalAggregation.ReduceContext.forFinalReduction(
            aggregator.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    private InternalAggregation finalizeReduction(Aggregator aggregator) throws IOException {
        InternalAggregation result = aggregator.buildTopLevel();
        InternalAggregation.ReduceContext context = createReduceContext(aggregator);

        @SuppressWarnings("unchecked")
        InternalAggregation reduced = result.reduce(List.of(result), context);
        reduced = reduced.reducePipelines(reduced, context, PipelineAggregator.PipelineTree.EMPTY);
        for (PipelineAggregator pipelineAggregator : PipelineAggregator.PipelineTree.EMPTY.aggregators()) {
            reduced = pipelineAggregator.reduce(reduced, context);
        }

        return reduced;
    }
}
