/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.AbstractWireTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.tsdb.query.aggregator.InternalTimeSeries.VERSION_2;

/**
 * Serialization tests for InternalTimeSeries.
 * Extends AbstractWireTestCase to automatically test wire serialization,
 * equals, and hashCode.
 */
public class InternalTimeSeriesSerializationTests extends AbstractWireTestCase<InternalTimeSeries> {

    @Override
    protected InternalTimeSeries createTestInstance() {
        String name = randomAlphaOfLength(10);

        // Create random metadata
        Map<String, Object> metadata = randomBoolean() ? null : createRandomMetadata();

        // Create random time series list
        List<TimeSeries> timeSeries = randomBoolean() ? new ArrayList<>() : createRandomTimeSeries();

        // Optionally include a reduce stage
        UnaryPipelineStage reduceStage = randomBoolean() ? null : createRandomReduceStage();

        // Optionally include exec stats (for V2 testing)
        AggregationExecStats execStats = randomBoolean()
            ? AggregationExecStats.EMPTY
            : new AggregationExecStats(
                randomLongBetween(0, 1000),
                randomLongBetween(0, 1000),
                randomLongBetween(0, 1000),
                randomLongBetween(0, 1000),
                randomLongBetween(0, 1000),
                randomLongBetween(0, 1000),
                randomLongBetween(0, 1000)
            );

        // Optionally include data source (for V2 testing)
        AggregationDataSource dataSource = randomBoolean()
            ? AggregationDataSource.EMPTY
            : new AggregationDataSource(
                Set.of(randomAlphaOfLength(5)),
                Set.of(new AggregationDataSource.IndexInfo(randomAlphaOfLength(3), randomAlphaOfLength(3)))
            );

        return new InternalTimeSeries(name, timeSeries, metadata, reduceStage, execStats, dataSource);
    }

    @Override
    protected InternalTimeSeries copyInstance(InternalTimeSeries instance, Version version) throws IOException {
        // Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new InternalTimeSeries(in);
            }
        }
    }

    @Override
    protected InternalTimeSeries mutateInstance(InternalTimeSeries instance) {
        // Always mutate the name to guarantee a different instance
        // This is the simplest and most reliable approach
        String name = (instance.getName() != null ? instance.getName() : "test") + "_mutated";

        return new InternalTimeSeries(name, instance.getTimeSeries(), instance.getMetadata(), instance.getReduceStage());
    }

    @Before
    public void setSerialVersion() {
        InternalTimeSeries.serialFormatSetting = InternalTimeSeries.VERSION_1;
    }

    @After
    public void resetSerialVersion() {
        InternalTimeSeries.serialFormatSetting = InternalTimeSeries.VERSION_0;
    }

    /**
     * Test serialization with null metadata.
     */
    public void testSerializationWithNullMetadata() throws IOException {
        // Arrange
        List<TimeSeries> timeSeries = createRandomTimeSeries();
        InternalTimeSeries original = new InternalTimeSeries("test", timeSeries, null);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertNull(deserialized.getMetadata());
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
            }
        }
    }

    /**
     * Test serialization with mixed sample types (FloatSample and SumCountSample).
     */
    public void testSerializationWithMixedSamples() throws IOException {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "mixed"));
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0f), new SumCountSample(2000L, 40.0, 2), new FloatSample(3000L, 30.0f));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "mixed-series");
        InternalTimeSeries original = new InternalTimeSeries("test_mixed", List.of(timeSeries), Map.of("key", "value"));

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());

                TimeSeries origSeries = original.getTimeSeries().get(0);
                TimeSeries deserSeries = deserialized.getTimeSeries().get(0);

                assertEquals(origSeries.getSamples().size(), deserSeries.getSamples().size());

                // Verify mixed sample types are preserved
                for (int i = 0; i < origSeries.getSamples().size(); i++) {
                    assertEquals(origSeries.getSamples().getSampleType(), deserSeries.getSamples().getSampleType());
                    assertEquals(origSeries.getSamples().getTimestamp(i), deserSeries.getSamples().getTimestamp(i));
                    assertEquals(origSeries.getSamples().getValue(i), deserSeries.getSamples().getValue(i), 0.001);

                    // Now we don't really explicitly support mix type of samples from our interface for simplicity
                    // Although it is still implicitly supported by using List<Sample> but there's no API to tell whether
                    // the list is of mixed sample type
                    // We should revisit if we see such a need of mixing sample types

                    // if (origSample instanceof SumCountSample origSumCount) {
                    // SumCountSample deserSumCount = (SumCountSample) deserSample;
                    // assertEquals(origSumCount.sum(), deserSumCount.sum(), 0.001);
                    // assertEquals(origSumCount.count(), deserSumCount.count());
                    // }
                }
            }
        }
    }

    /**
     * Test serialization with reduce stage.
     */
    public void testSerializationWithReduceStage() throws IOException {
        // Arrange
        List<TimeSeries> timeSeries = createRandomTimeSeries();
        UnaryPipelineStage reduceStage = new SumStage("service");
        InternalTimeSeries original = new InternalTimeSeries("test_reduce", timeSeries, Map.of("key", "value"), reduceStage);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMetadata(), deserialized.getMetadata());
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
                // Verify reduce stage is preserved
                assertNotNull(deserialized.getReduceStage());
                assertEquals(original.getReduceStage().getName(), deserialized.getReduceStage().getName());
            }
        }
    }

    public void testSerializationWithFloatSampleList() throws IOException {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();
        for (int i = 0; i < 10; i++) {
            builder.add(i, i * 2);
        }
        TimeSeries ts = new TimeSeries(builder.build(), ByteLabels.emptyLabels(), 0, 9, 1, "aaa");
        InternalTimeSeries original = new InternalTimeSeries("test", List.of(ts), Map.of("key", "value"));

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMetadata(), deserialized.getMetadata());
                assertEquals(deserialized.getTimeSeries().get(0).getSamples(), ts.getSamples());
            }
        }
    }

    /**
     * Test that EMPTY exec stats round-trips correctly in V2.
     */
    public void testSerializationWithEmptyExecStats() throws IOException {
        // Arrange
        InternalTimeSeries.serialFormatSetting = VERSION_2;
        InternalTimeSeries original = new InternalTimeSeries("test_empty_exec", new ArrayList<>(), Map.of());

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);
                // Assert - EMPTY should round-trip back to EMPTY
                assertEquals(AggregationExecStats.EMPTY, deserialized.getExecStats());
                assertEquals(AggregationDataSource.EMPTY, deserialized.getDataSource());
            }
        }
    }

    /**
     * Test that EMPTY data source round-trips correctly in V2.
     */
    public void testSerializationWithEmptyDataSource() throws IOException {
        // Arrange
        InternalTimeSeries.serialFormatSetting = VERSION_2;
        InternalTimeSeries original = new InternalTimeSeries("test_empty_ds", new ArrayList<>(), Map.of());

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);
                // Assert - EMPTY should round-trip back to EMPTY
                assertEquals(AggregationDataSource.EMPTY, deserialized.getDataSource());
            }
        }
    }

    /**
     * Test serialization with empty time series list.
     */
    public void testSerializationWithEmptyTimeSeries() throws IOException {
        // Arrange
        InternalTimeSeries original = new InternalTimeSeries("test_empty", new ArrayList<>(), Map.of("key", "value"));

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMetadata(), deserialized.getMetadata());
                assertTrue(deserialized.getTimeSeries().isEmpty());
            }
        }
    }

    /**
     * Test cross-version compatibility: write with each version, read with each version.
     * The reader auto-detects the wire format from the sentinel byte, so the read-side
     * serialFormatSetting should not affect correctness.
     * V0/V1 writes should yield EMPTY exec stats on read; V2 writes should preserve them.
     */
    public void testBackCompatibility() throws IOException {
        List<Integer> versions = new ArrayList<>(InternalTimeSeries.SUPPORTED_VERSIONS);
        Collections.sort(versions);
        for (int writeVersion : versions) {
            for (int epoch = 0; epoch < 16; epoch++) {
                InternalTimeSeries original = createTestInstance();
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    InternalTimeSeries.serialFormatSetting = writeVersion;
                    original.writeTo(out);

                    try (StreamInput in = out.bytes().streamInput()) {
                        InternalTimeSeries deserialized = new InternalTimeSeries(in);

                        // Assert
                        assertEquals(original.getName(), deserialized.getName());
                        assertEquals(original.getMetadata(), deserialized.getMetadata());
                        assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
                        for (int i = 0; i < deserialized.getTimeSeries().size(); i++) {
                            assertEquals(original.getTimeSeries().get(i), deserialized.getTimeSeries().get(i));
                        }
                        if (writeVersion >= 2) {
                            // V2 writes preserve exec stats and data source
                            assertEquals(original.getExecStats(), deserialized.getExecStats());
                            assertEquals(original.getDataSource(), deserialized.getDataSource());
                        } else {
                            // V0 and V1 writes yield EMPTY exec stats and data source
                            assertEquals(AggregationExecStats.EMPTY, deserialized.getExecStats());
                            assertEquals(AggregationDataSource.EMPTY, deserialized.getDataSource());
                        }
                    }
                }
            }
        }
    }

    /**
     * Test V2 write -> V2 read preserves exec stats.
     */
    public void testV2RoundTripWithExecStats() throws IOException {
        // Change to V2
        InternalTimeSeries.serialFormatSetting = VERSION_2;
        for (int epoch = 0; epoch < 8; epoch++) {
            AggregationExecStats execStats = new AggregationExecStats(
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000)
            );
            List<TimeSeries> ts = createRandomTimeSeries();
            UnaryPipelineStage reduceStage = randomBoolean() ? null : createRandomReduceStage();
            InternalTimeSeries original = new InternalTimeSeries(
                "test_v2",
                ts,
                Map.of("k", "v"),
                reduceStage,
                execStats,
                AggregationDataSource.EMPTY
            );

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    InternalTimeSeries deserialized = new InternalTimeSeries(in);
                    assertEquals(execStats, deserialized.getExecStats());
                    assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
                    if (reduceStage != null) {
                        assertNotNull(deserialized.getReduceStage());
                        assertEquals(reduceStage.getName(), deserialized.getReduceStage().getName());
                    } else {
                        assertNull(deserialized.getReduceStage());
                    }
                }
            }
        }
    }

    /**
     * Test V2 write -> V2 read preserves data source.
     */
    public void testV2RoundTripWithDataSource() throws IOException {
        // Change to V2
        InternalTimeSeries.serialFormatSetting = VERSION_2;
        for (int epoch = 0; epoch < 8; epoch++) {
            AggregationDataSource dataSource = new AggregationDataSource(
                Set.of(randomAlphaOfLength(5)),
                Set.of(new AggregationDataSource.IndexInfo(randomAlphaOfLength(3), randomAlphaOfLength(3)))
            );
            List<TimeSeries> ts = createRandomTimeSeries();
            UnaryPipelineStage reduceStage = randomBoolean() ? null : createRandomReduceStage();
            InternalTimeSeries original = new InternalTimeSeries(
                "test_v2_ds",
                ts,
                Map.of("k", "v"),
                reduceStage,
                AggregationExecStats.EMPTY,
                dataSource
            );

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    InternalTimeSeries deserialized = new InternalTimeSeries(in);
                    assertEquals(dataSource, deserialized.getDataSource());
                    assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
                }
            }
        }
    }

    /**
     * Test that an unknown sentinel version throws.
     */
    public void testUnknownVersionThrows() throws IOException {
        // Create a stream with sentinel -99 (unsupported version)
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write InternalAggregation header manually: name string + empty metadata map
            out.writeString("test");
            out.writeGenericValue(null); // metadata (null map)
            // Write unsupported sentinel
            out.writeVInt(-99);
            try (StreamInput in = out.bytes().streamInput()) {
                expectThrows(IllegalStateException.class, () -> new InternalTimeSeries(in));
            }
        }
    }

    // ========== Helper Methods ==========

    private Map<String, Object> createRandomMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        int numEntries = randomIntBetween(1, 5);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(5);
            Object value = randomBoolean() ? randomAlphaOfLength(8) : randomIntBetween(1, 100);
            metadata.put(key, value);
        }
        return metadata;
    }

    private List<TimeSeries> createRandomTimeSeries() {
        List<TimeSeries> timeSeries = new ArrayList<>();
        int numSeries = randomIntBetween(1, 3);

        for (int i = 0; i < numSeries; i++) {
            // Create random labels
            Map<String, String> labelMap = new HashMap<>();
            labelMap.put("service", randomAlphaOfLength(5));
            labelMap.put("region", randomAlphaOfLength(5));
            Labels labels = ByteLabels.fromMap(labelMap);

            // Create random samples
            int numSamples = randomIntBetween(1, 10);
            long baseTimestamp = randomLongBetween(1000L, 10000L);
            SampleList sampleList;
            if (randomBoolean()) {
                FloatSampleList.Builder builder = new FloatSampleList.Builder();
                for (int j = 0; j < numSamples; j++) {
                    long timestamp = baseTimestamp + j * 1000L;
                    double value = randomDoubleBetween(0.0, 100.0, true);
                    builder.add(timestamp, value);
                }
                sampleList = builder.build();
            } else {
                List<Sample> samples = new ArrayList<>();
                for (int j = 0; j < numSamples; j++) {
                    long timestamp = baseTimestamp + j * 1000L;
                    double value = randomDoubleBetween(0.0, 100.0, true);

                    // Randomly use FloatSample or SumCountSample
                    if (randomBoolean()) {
                        samples.add(new FloatSample(timestamp, (float) value));
                    } else {
                        samples.add(new SumCountSample(timestamp, value, randomIntBetween(1, 10)));
                    }
                }
                sampleList = SampleList.fromList(samples);
            }

            long minTimestamp = baseTimestamp;
            long maxTimestamp = baseTimestamp + (numSamples - 1) * 1000L;
            String alias = randomBoolean() ? null : randomAlphaOfLength(8);

            timeSeries.add(new TimeSeries(sampleList, labels, minTimestamp, maxTimestamp, 1000L, alias));
        }

        return timeSeries;
    }

    private UnaryPipelineStage createRandomReduceStage() {
        // Randomly choose between different reduce stage types
        if (randomBoolean()) {
            return new SumStage(randomAlphaOfLength(5));
        } else {
            return new ScaleStage(randomDoubleBetween(0.1, 10.0, true));
        }
    }

    private TimeSeries createSingleTimeSeries() {
        // Create a single time series with minimal data
        Map<String, String> labelMap = new HashMap<>();
        labelMap.put("service", randomAlphaOfLength(5));
        Labels labels = ByteLabels.fromMap(labelMap);

        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, (float) randomDoubleBetween(0.0, 100.0, true)));

        return new TimeSeries(samples, labels, 1000L, 1000L, 1000L, randomAlphaOfLength(5));
    }
}
