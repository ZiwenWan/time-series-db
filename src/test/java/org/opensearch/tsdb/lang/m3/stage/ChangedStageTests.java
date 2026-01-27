/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangedStageTests extends AbstractWireSerializingTestCase<ChangedStage> {

    public void testProcessWithEmptyInput() {
        ChangedStage stage = new ChangedStage();
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        ChangedStage stage = new ChangedStage();
        TestUtils.assertNullInputThrowsException(stage, "changed");
    }

    public void testProcessBasicChanged() {
        ChangedStage stage = new ChangedStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "actions", "city", "atlanta");
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),  // first value -> 0 (no previous value)
            new FloatSample(2000L, 10.0),  // same as previous -> 0
            new FloatSample(3000L, 20.0),  // changed from 10 -> 1
            new FloatSample(4000L, 20.0),  // same as previous -> 0
            new FloatSample(5000L, 15.0)   // changed from 20 -> 1
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 0.0),   // first value, no previous
            new FloatSample(2000L, 0.0),   // same as previous (10.0)
            new FloatSample(3000L, 1.0),   // changed (10.0 -> 20.0)
            new FloatSample(4000L, 0.0),   // same as previous (20.0)
            new FloatSample(5000L, 1.0)    // changed (20.0 -> 15.0)
        );
        assertSamplesEqual("Basic changed", expectedSamples, result.get(0).getSamples());
    }

    public void testProcessWithNaNValues() {
        ChangedStage stage = new ChangedStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "bookings", "dc", "dca8");
        List<Sample> samples = List.of(
            new FloatSample(1000L, Double.NaN), // NaN -> 0
            new FloatSample(2000L, 20.0),       // first non-NaN value -> 0
            new FloatSample(3000L, Double.NaN), // NaN -> 0
            new FloatSample(4000L, 20.0),       // same as last non-NaN -> 0
            new FloatSample(5000L, 30.0)        // changed from last non-NaN -> 1
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 0.0),   // NaN value -> 0
            new FloatSample(2000L, 0.0),   // first non-NaN value -> 0
            new FloatSample(3000L, 0.0),   // NaN value -> 0
            new FloatSample(4000L, 0.0),   // same as last non-NaN (20.0)
            new FloatSample(5000L, 1.0)    // changed from last non-NaN (20.0 -> 30.0)
        );
        assertSamplesEqual("Changed with NaN values", expectedSamples, result.get(0).getSamples());
    }

    public void testProcessWithAllNaNValues() {
        ChangedStage stage = new ChangedStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(
            new FloatSample(1000L, Double.NaN),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, Double.NaN)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(new FloatSample(1000L, 0.0), new FloatSample(2000L, 0.0), new FloatSample(3000L, 0.0));
        assertSamplesEqual("All NaN values", expectedSamples, result.get(0).getSamples());
    }

    public void testProcessWithSingleSample() {
        ChangedStage stage = new ChangedStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 0.0) // single sample, no previous value
        );
        assertSamplesEqual("Single sample", expectedSamples, result.get(0).getSamples());
    }

    public void testProcessWithEmptySamples() {
        ChangedStage stage = new ChangedStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries series = new TimeSeries(new ArrayList<>(), labels, 1000L, 1000L, 1000L, null);
        List<TimeSeries> input = List.of(series);

        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertTrue(result.get(0).getSamples().isEmpty());
    }

    public void testProcessTransitionFromNaN() {
        ChangedStage stage = new ChangedStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(
            new FloatSample(1000L, Double.NaN), // NaN -> 0
            new FloatSample(2000L, Double.NaN), // NaN -> 0
            new FloatSample(3000L, 10.0),       // first valid value -> 0
            new FloatSample(4000L, 10.0),       // same -> 0
            new FloatSample(5000L, 15.0)        // different -> 1
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 0.0),
            new FloatSample(2000L, 0.0),
            new FloatSample(3000L, 0.0), // first non-NaN value
            new FloatSample(4000L, 0.0), // same as previous
            new FloatSample(5000L, 1.0)  // changed
        );
        assertSamplesEqual("Transition from NaN", expectedSamples, result.get(0).getSamples());
    }

    public void testFromArgs() {
        ChangedStage stage = ChangedStage.fromArgs(Map.of());
        assertEquals("changed", stage.getName());
    }

    public void testSupportConcurrentSegmentSearch() {
        ChangedStage stage = new ChangedStage();
        assertFalse("Changed stage should not support concurrent segment search", stage.supportConcurrentSegmentSearch());
    }

    @Override
    protected ChangedStage createTestInstance() {
        return new ChangedStage();
    }

    @Override
    protected Writeable.Reader<ChangedStage> instanceReader() {
        return ChangedStage::readFrom;
    }
}
