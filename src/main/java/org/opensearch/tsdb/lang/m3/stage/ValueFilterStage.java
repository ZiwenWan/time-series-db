/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that filters time series values based on comparison operators.
 *
 * This stage filters time series samples by comparing their values against a target value
 * using various comparison operators. It supports equality, inequality, and relational
 * comparisons, making it a versatile filtering tool for time series data.
 *
 * <h2>Supported Operators:</h2>
 * <ul>
 *   <li><strong>eq/==</strong>: Equal to (with floating-point tolerance)</li>
 *   <li><strong>ne/!=</strong>: Not equal to</li>
 *   <li><strong>ge/>=</strong>: Greater than or equal to</li>
 *   <li><strong>gt/></strong>: Greater than</li>
 *   <li><strong>le/&lt;=</strong>: Less than or equal to</li>
 *   <li><strong>lt/&lt;</strong>: Less than</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Filter values equal to 10.5
 * ValueFilterStage eqFilter = new ValueFilterStage(Operator.EQ, 10.5);
 *
 * // Filter values greater than 100
 * ValueFilterStage gtFilter = new ValueFilterStage(Operator.GT, 100.0);
 *
 * // Filter values less than or equal to 50
 * ValueFilterStage leFilter = new ValueFilterStage(Operator.LE, 50.0);
 * }</pre>
 *
 */
@PipelineStageAnnotation(name = "valueFilter")
public class ValueFilterStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "valueFilter";
    /** The argument name for operator parameter. */
    public static final String OPERATOR_ARG = "operator";
    /** The argument name for target value parameter. */
    public static final String TARGET_VALUE_ARG = "targetValue";

    /**
     * Enumeration of supported comparison operators.
     */
    public enum Operator {
        /** Equal to (with floating-point tolerance) */
        EQ("eq", "=="),
        /** Not equal to */
        NE("ne", "!="),
        /** Greater than or equal to */
        GE("ge", ">="),
        /** Greater than */
        GT("gt", ">"),
        /** Less than or equal to */
        LE("le", "<="),
        /** Less than */
        LT("lt", "<");

        private final String name;
        private final String symbol;

        Operator(String name, String symbol) {
            this.name = name;
            this.symbol = symbol;
        }

        /**
         * Get the string representation of this operator.
         * @return the name of the operator
         */
        public String getName() {
            return name;
        }

        /**
         * Get the symbol representation of this operator.
         * @return the symbol of the operator
         */
        public String getSymbol() {
            return symbol;
        }

        /**
         * Parse a string into an Operator enum value.
         * @param name the string representation (case sensitive)
         * @return the corresponding Operator enum value
         * @throws IllegalArgumentException if the name is not recognized
         */
        public static Operator fromString(String name) {
            for (Operator operator : values()) {
                if (operator.name.equals(name) || operator.symbol.equals(name)) {
                    return operator;
                }
            }
            throw new IllegalArgumentException("Unknown operator: " + name + ". Supported: eq/==, ne/!=, ge/>=, gt/>, le/<=, lt/<");
        }
    }

    private final Operator operator;
    private final double targetValue;

    /**
     * Constructs a new ValueFilterStage with the specified operator and target value.
     *
     * @param operator the comparison operator to use
     * @param targetValue the target value to compare against
     */
    public ValueFilterStage(Operator operator, double targetValue) {
        this.operator = operator;
        this.targetValue = targetValue;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException("Input cannot be null");
        }
        List<TimeSeries> result = new ArrayList<>();
        for (TimeSeries series : input) {
            List<Sample> filteredSamples = new ArrayList<>();
            for (Sample sample : series.getSamples()) {
                if (sample != null && !Double.isNaN(sample.getValue())) {
                    if (matchesCondition(sample.getValue())) {
                        filteredSamples.add(sample);
                    }
                }
            }
            result.add(
                new TimeSeries(
                    filteredSamples,
                    series.getLabels(),
                    series.getMinTimestamp(),
                    series.getMaxTimestamp(),
                    series.getStep(),
                    series.getAlias()
                )
            );
        }
        return result;
    }

    /**
     * Check if a value matches the filter condition.
     *
     * @param value the value to check
     * @return true if the value matches the condition, false otherwise
     */
    private boolean matchesCondition(double value) {
        return switch (operator) {
            case EQ -> value == targetValue;
            case NE -> value != targetValue;
            case GE -> value >= targetValue;
            case GT -> value > targetValue;
            case LE -> value <= targetValue;
            case LT -> value < targetValue;
        };
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the comparison operator.
     * @return the operator
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Get the target value.
     * @return the target value
     */
    public double getTargetValue() {
        return targetValue;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(OPERATOR_ARG, operator.getName());
        builder.field(TARGET_VALUE_ARG, targetValue);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(operator.getName());
        out.writeDouble(targetValue);
    }

    /**
     * Create a ValueFilterStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ValueFilterStage instance with the deserialized parameters
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static ValueFilterStage readFrom(StreamInput in) throws IOException {
        String operatorName = in.readString();
        double targetValue = in.readDouble();

        Operator operator = Operator.fromString(operatorName);
        return new ValueFilterStage(operator, targetValue);
    }

    /**
     * Create a ValueFilterStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return ValueFilterStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static ValueFilterStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(OPERATOR_ARG)) {
            throw new IllegalArgumentException("ValueFilter stage requires '" + OPERATOR_ARG + "' argument");
        }
        if (!args.containsKey(TARGET_VALUE_ARG)) {
            throw new IllegalArgumentException("ValueFilter stage requires '" + TARGET_VALUE_ARG + "' argument");
        }

        Object operatorObj = args.get(OPERATOR_ARG);
        if (operatorObj == null) {
            throw new IllegalArgumentException("Operator cannot be null");
        }
        if (!(operatorObj instanceof String)) {
            throw new IllegalArgumentException(
                "Invalid type for '" + OPERATOR_ARG + "' argument. Expected String, but got " + operatorObj.getClass().getSimpleName()
            );
        }

        Object targetValueObj = args.get(TARGET_VALUE_ARG);
        if (targetValueObj == null) {
            throw new IllegalArgumentException("Target value cannot be null");
        }
        if (!(targetValueObj instanceof Number)) {
            throw new IllegalArgumentException(
                "Invalid type for '"
                    + TARGET_VALUE_ARG
                    + "' argument. Expected Number, but got "
                    + targetValueObj.getClass().getSimpleName()
            );
        }

        Operator operator = Operator.fromString((String) operatorObj);
        double targetValue = ((Number) targetValueObj).doubleValue();

        return new ValueFilterStage(operator, targetValue);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return true;
    }
}
