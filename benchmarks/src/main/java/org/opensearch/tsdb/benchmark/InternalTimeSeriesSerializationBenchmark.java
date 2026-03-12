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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class InternalTimeSeriesSerializationBenchmark {
    private static final long MIN_TIME = 123450000;
    private static final long STEP = 10000;

    final static Map<String, String> LABEL_OF_10_STRING = IntStream.rangeClosed(1, 10)
        .boxed()
        .collect(
            Collectors.toMap(
                // Key Mapper: Function to generate the map key (e.g., 1 -> "key_1")
                i -> "key_" + i,
                // Value Mapper: Function to generate the map value (e.g., 1 -> "value_1")
                i -> "value_" + i
            )
        );
    public final ByteLabels STATIC_LABELS = ByteLabels.fromMap(LABEL_OF_10_STRING);

    @Param({ "500" })
    public int samplesPerSeries;

    @Param({ "1000" })
    public int numSeries;

    @Param({ "16" })
    public int numShards;

    private InternalTimeSeries[] internalTimeSeries;
    private byte[][] serializedTimeSeries;
    private byte[][] legacySerializedTimeSeries;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        Random random = new Random();
        internalTimeSeries = new InternalTimeSeries[numShards];
        serializedTimeSeries = new byte[numShards][];
        legacySerializedTimeSeries = new byte[numShards][];
        for (int s = 0; s < numShards; s++) {
            List<TimeSeries> seriesList = new ArrayList<>();
            for (int i = 0; i < numSeries; i++) {
                FloatSampleList.Builder builder = new FloatSampleList.Builder(samplesPerSeries);
                for (int j = 0; j < samplesPerSeries; j++) {
                    builder.add(MIN_TIME + j * STEP, random.nextDouble());
                }
                TimeSeries ts = new TimeSeries(
                    builder.build(),
                    STATIC_LABELS,
                    MIN_TIME,
                    MIN_TIME + (samplesPerSeries - 1) * STEP,
                    STEP,
                    ""
                );
                seriesList.add(ts);
            }
            internalTimeSeries[s] = new InternalTimeSeries("test-" + s, seriesList, Map.of());
            InternalTimeSeries.serialFormatSetting = InternalTimeSeries.VERSION_1;
            BytesStreamOutput out = new BytesStreamOutput();
            internalTimeSeries[s].writeTo(out);
            serializedTimeSeries[s] = BytesReference.toBytes(out.bytes());
            // System.out.println(RamUsageEstimator.sizeOf(serializedTimeSeries[s]));

            out = new BytesStreamOutput();
            InternalTimeSeries.serialFormatSetting = InternalTimeSeries.VERSION_0;
            internalTimeSeries[s].writeTo(out);
            legacySerializedTimeSeries[s] = BytesReference.toBytes(out.bytes());
            // System.out.println(RamUsageEstimator.sizeOf(legacySerializedTimeSeries[s]));
        }
    }

    @Benchmark
    public void serialBench(Blackhole bh) throws IOException {
        InternalTimeSeries.serialFormatSetting = InternalTimeSeries.VERSION_1;
        for (int s = 0; s < numShards; s++) {
            BytesStreamOutput out = new BytesStreamOutput();
            internalTimeSeries[s].writeTo(out);
        }
    }

    @Benchmark
    public void deserialBench(Blackhole bh) throws IOException {
        for (int s = 0; s < numShards; s++) {
            bh.consume(new InternalTimeSeries(new BytesStreamInput(serializedTimeSeries[s])));
        }
    }

    @Benchmark
    public void oldSerialBench(Blackhole bh) throws IOException {
        InternalTimeSeries.serialFormatSetting = InternalTimeSeries.VERSION_0;
        for (int s = 0; s < numShards; s++) {
            BytesStreamOutput out = new BytesStreamOutput();
            internalTimeSeries[s].writeTo(out);
        }
    }

    @Benchmark
    public void oldDeserialBench(Blackhole bh) throws IOException {
        for (int s = 0; s < numShards; s++) {
            bh.consume(new InternalTimeSeries(new BytesStreamInput(legacySerializedTimeSeries[s])));
        }
    }
}
