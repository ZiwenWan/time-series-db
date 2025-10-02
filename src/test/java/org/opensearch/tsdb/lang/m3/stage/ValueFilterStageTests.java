/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ValueFilterStageTests extends OpenSearchTestCase {

    // ========== Constructor Tests ==========

    public void testConstructorWithOperatorAndTargetValue() {
        // Arrange & Act
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.EQ, 10.5);

        // Assert
        assertEquals(ValueFilterStage.Operator.EQ, filterStage.getOperator());
        assertEquals(10.5, filterStage.getTargetValue(), 0.001);
        assertEquals("valueFilter", filterStage.getName());
    }

    // ========== Process Method Tests ==========

    public void testProcessWithNullInput() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.EQ, 10.0);

        // Act & Assert
        NullPointerException exception = expectThrows(NullPointerException.class, () -> filterStage.process(null));
        assertEquals("Input cannot be null", exception.getMessage());
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.EQ, 10.0);
        List<TimeSeries> input = new ArrayList<>();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testProcessEqOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.EQ, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().get(0).getValue(), 0.001);
    }

    public void testProcessNeOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.NE, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().get(0).getValue(), 0.001);
        assertEquals(15.0, result.get(0).getSamples().get(1).getValue(), 0.001);
    }

    public void testProcessGtOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.GT, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(15.0, result.get(0).getSamples().get(0).getValue(), 0.001);
    }

    public void testProcessGeOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.GE, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().get(0).getValue(), 0.001);
        assertEquals(15.0, result.get(0).getSamples().get(1).getValue(), 0.001);
    }

    public void testProcessLtOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.LT, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().get(0).getValue(), 0.001);
    }

    public void testProcessLeOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.LE, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().get(0).getValue(), 0.001);
        assertEquals(10.0, result.get(0).getSamples().get(1).getValue(), 0.001);
    }

    public void testProcessWithNaNValues() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.EQ, 10.0);
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 10.0),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, 15.0)
        );
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().get(0).getValue(), 0.001);
    }

    public void testProcessWithNullSamples() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterStage.Operator.EQ, 10.0);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), null, new FloatSample(3000L, 15.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().get(0).getValue(), 0.001);
    }

    // ========== Operator Enum Tests ==========

    public void testOperatorFromString() {
        assertEquals(ValueFilterStage.Operator.EQ, ValueFilterStage.Operator.fromString("eq"));
        assertEquals(ValueFilterStage.Operator.EQ, ValueFilterStage.Operator.fromString("=="));
        assertEquals(ValueFilterStage.Operator.NE, ValueFilterStage.Operator.fromString("ne"));
        assertEquals(ValueFilterStage.Operator.NE, ValueFilterStage.Operator.fromString("!="));
        assertEquals(ValueFilterStage.Operator.GT, ValueFilterStage.Operator.fromString("gt"));
        assertEquals(ValueFilterStage.Operator.GT, ValueFilterStage.Operator.fromString(">"));
        assertEquals(ValueFilterStage.Operator.GE, ValueFilterStage.Operator.fromString("ge"));
        assertEquals(ValueFilterStage.Operator.GE, ValueFilterStage.Operator.fromString(">="));
        assertEquals(ValueFilterStage.Operator.LT, ValueFilterStage.Operator.fromString("lt"));
        assertEquals(ValueFilterStage.Operator.LT, ValueFilterStage.Operator.fromString("<"));
        assertEquals(ValueFilterStage.Operator.LE, ValueFilterStage.Operator.fromString("le"));
        assertEquals(ValueFilterStage.Operator.LE, ValueFilterStage.Operator.fromString("<="));
    }

    public void testOperatorFromStringCaseSensitive() {
        // Test that uppercase operators are not accepted
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.Operator.fromString("EQ"));
        assertEquals("Unknown operator: EQ. Supported: eq/==, ne/!=, ge/>=, gt/>, le/<=, lt/<", exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.Operator.fromString("GT"));
        assertEquals("Unknown operator: GT. Supported: eq/==, ne/!=, ge/>=, gt/>, le/<=, lt/<", exception.getMessage());
    }

    public void testOperatorFromStringInvalid() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ValueFilterStage.Operator.fromString("invalid")
        );
        assertEquals("Unknown operator: invalid. Supported: eq/==, ne/!=, ge/>=, gt/>, le/<=, lt/<", exception.getMessage());
    }

    // ========== Serialization Tests ==========

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        ValueFilterStage original = new ValueFilterStage(ValueFilterStage.Operator.GT, 5.0);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        original.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ValueFilterStage deserialized = ValueFilterStage.readFrom(input);

        // Assert
        assertEquals(original.getOperator(), deserialized.getOperator());
        assertEquals(original.getTargetValue(), deserialized.getTargetValue(), 0.001);
    }

    public void testToXContent() throws IOException {
        // Test XContent serialization
        ValueFilterStage stage = new ValueFilterStage(ValueFilterStage.Operator.GE, 10.5);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue("JSON should contain operator field", json.contains(ValueFilterStage.OPERATOR_ARG));
        assertTrue("JSON should contain targetValue field", json.contains(ValueFilterStage.TARGET_VALUE_ARG));
        assertTrue("JSON should contain ge", json.contains("ge"));
        assertTrue("JSON should contain 10.5", json.contains("10.5"));
    }

    // ========== FromArgs Tests ==========

    public void testFromArgsValid() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "gt", ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act
        ValueFilterStage filterStage = ValueFilterStage.fromArgs(args);

        // Assert
        assertEquals(ValueFilterStage.Operator.GT, filterStage.getOperator());
        assertEquals(10.5, filterStage.getTargetValue(), 0.001);
    }

    public void testFromArgsWithSymbolOperator() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, ">=", ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act
        ValueFilterStage filterStage = ValueFilterStage.fromArgs(args);

        // Assert
        assertEquals(ValueFilterStage.Operator.GE, filterStage.getOperator());
        assertEquals(10.5, filterStage.getTargetValue(), 0.001);
    }

    public void testFromArgsMissingOperator() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("ValueFilter stage requires '" + ValueFilterStage.OPERATOR_ARG + "' argument"));
    }

    public void testFromArgsMissingTargetValue() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "eq");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("ValueFilter stage requires '" + ValueFilterStage.TARGET_VALUE_ARG + "' argument"));
    }

    public void testFromArgsNullArgs() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(null));
        assertTrue(exception.getMessage().contains("ValueFilter stage requires '" + ValueFilterStage.OPERATOR_ARG + "' argument"));
    }

    public void testFromArgsNullOperator() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(ValueFilterStage.OPERATOR_ARG, null);
        args.put(ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertEquals("Operator cannot be null", exception.getMessage());
    }

    public void testFromArgsNullTargetValue() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(ValueFilterStage.OPERATOR_ARG, "eq");
        args.put(ValueFilterStage.TARGET_VALUE_ARG, null);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertEquals("Target value cannot be null", exception.getMessage());
    }

    public void testFromArgsInvalidOperatorType() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, 123, ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + ValueFilterStage.OPERATOR_ARG + "' argument"));
    }

    public void testFromArgsInvalidTargetValueType() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "eq", ValueFilterStage.TARGET_VALUE_ARG, "invalid");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + ValueFilterStage.TARGET_VALUE_ARG + "' argument"));
    }

    public void testFromArgsInvalidOperator() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "invalid", ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Unknown operator"));
    }

    // ========== Helper Methods ==========

    private List<TimeSeries> createTestTimeSeries() {
        // Create time series with values: [5.0, 10.0, 15.0]
        TimeSeries series = createTimeSeries(Arrays.asList(5.0, 10.0, 15.0));
        return Arrays.asList(series);
    }

    private TimeSeries createTimeSeries(List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        return new TimeSeries(samples, labels, 1000L, 1000L + (values.size() - 1) * 1000L, 1000L, "test");
    }
}
