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
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's changed function.
 *
 * Takes one metric or a wildcard seriesList. Outputs 1 when the value changed
 * and outputs 0 when the value is null or the same. Each sample in the time
 * series is processed and outputs either 0 or 1 based on comparison with the
 * prior non-null and non-NaN value.
 *
 * This stage requires information from prior samples and does not support
 * concurrent segment search.
 *
 * Usage: fetch a | changed
 */
@PipelineStageAnnotation(name = "changed")
public class ChangedStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "changed";

    /**
     * Constructor for ChangedStage.
     */
    public ChangedStage() {
        // No arguments needed
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>(input.size());

        for (TimeSeries ts : input) {
            List<Sample> samples = ts.getSamples();
            if (samples.isEmpty()) {
                result.add(ts);
                continue;
            }

            List<Sample> changedSamples = new ArrayList<>(samples.size());
            Double lastNonNullValue = null;

            for (Sample sample : samples) {
                long timestamp = sample.getTimestamp();
                double currentValue = sample.getValue();

                double outputValue;

                if (Double.isNaN(currentValue)) {
                    // Current value is null/NaN: output 0
                    outputValue = 0.0;
                } else {
                    // Current value is not null/NaN: compare with prior value
                    outputValue = (lastNonNullValue != null && currentValue != lastNonNullValue) ? 1.0 : 0.0;
                    lastNonNullValue = currentValue;
                }

                changedSamples.add(new FloatSample(timestamp, outputValue));
            }

            result.add(
                new TimeSeries(changedSamples, ts.getLabels(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getAlias())
            );
        }

        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        // This stage requires information from prior samples, so it cannot support
        // concurrent segment search
        return false;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters
    }

    /**
     * Create a ChangedStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ChangedStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static ChangedStage readFrom(StreamInput in) throws IOException {
        return new ChangedStage();
    }

    /**
     * Create a ChangedStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return ChangedStage instance
     */
    public static ChangedStage fromArgs(Map<String, Object> args) {
        return new ChangedStage();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }
}
