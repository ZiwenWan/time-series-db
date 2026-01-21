/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.util;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SortComparatorUtilTests extends OpenSearchTestCase {

    // ========== createComparator() Tests ==========

    public void testCreateComparatorForAvg() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(1.0, 3.0)); // avg=2.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(2.0, 6.0)); // avg=4.0
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.AVG);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: A < B (2.0 < 4.0)
        assertTrue("Series A should be less than Series B", result < 0);
    }

    public void testCreateComparatorForCurrent() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(1.0, 5.0)); // current=5.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(2.0, 3.0)); // current=3.0
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.CURRENT);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: A > B (5.0 > 3.0)
        assertTrue("Series A should be greater than Series B", result > 0);
    }

    public void testCreateComparatorForMax() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(1.0, 4.0)); // max=4.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(2.0, 6.0)); // max=6.0
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.MAX);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: A < B (4.0 < 6.0)
        assertTrue("Series A should be less than Series B", result < 0);
    }

    public void testCreateComparatorForMin() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(3.0, 8.0)); // min=3.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 5.0)); // min=1.0
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.MIN);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: A > B (3.0 > 1.0)
        assertTrue("Series A should be greater than Series B", result > 0);
    }

    public void testCreateComparatorForSum() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(1.0, 2.0)); // sum=3.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(2.0, 4.0)); // sum=6.0
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.SUM);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: A < B (3.0 < 6.0)
        assertTrue("Series A should be less than Series B", result < 0);
    }

    public void testCreateComparatorForStddev() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 5.0)); // stddev=0.0
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 10.0)); // stddev > 0
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.STDDEV);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: A < B (0.0 < stddev_B)
        assertTrue("Series A should be less than Series B", result < 0);
    }

    public void testCreateComparatorForName() {
        // Arrange
        TimeSeries seriesA = createTimeSeriesWithAlias("alpha", Arrays.asList(1.0));
        TimeSeries seriesB = createTimeSeriesWithAlias("bravo", Arrays.asList(2.0));
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.NAME);

        // Act
        int result = comparator.compare(seriesA, seriesB);

        // Assert: "alpha" < "bravo"
        assertTrue("Series A should be less than Series B alphabetically", result < 0);
    }

    // ========== calculateAverage() Tests ==========

    public void testCalculateAverageWithValidSamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));

        // Act
        double average = SortComparatorUtil.calculateAverage(timeSeries);

        // Assert
        assertEquals("Average should be 3.0", 3.0, average, 0.001);
    }

    public void testCalculateAverageWithEmptySamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("empty", new ArrayList<>());

        // Act
        double average = SortComparatorUtil.calculateAverage(timeSeries);

        // Assert
        assertEquals("Average of empty series should be 0.0", 0.0, average, 0.0);
    }

    public void testCalculateAverageWithNaNSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 3.0));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double average = SortComparatorUtil.calculateAverage(timeSeries);

        // Assert: Should ignore NaN and calculate average of valid values (1.0 + 3.0) / 2 = 2.0
        assertEquals("Average should ignore NaN values", 2.0, average, 0.001);
    }

    public void testCalculateAverageWithNullSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 2.0), null, new FloatSample(3000L, 4.0));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double average = SortComparatorUtil.calculateAverage(timeSeries);

        // Assert: Should ignore null and calculate average of valid values (2.0 + 4.0) / 2 = 3.0
        assertEquals("Average should ignore null values", 3.0, average, 0.001);
    }

    public void testCalculateAverageWithAllInvalidSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null, new FloatSample(3000L, Double.NaN));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double average = SortComparatorUtil.calculateAverage(timeSeries);

        // Assert: Should return 0.0 when no valid samples
        assertEquals("Average should be 0.0 when all samples are invalid", 0.0, average, 0.0);
    }

    // ========== calculateCurrent() Tests ==========

    public void testCalculateCurrentWithValidSamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(1.0, 2.0, 3.0, 4.0));

        // Act
        double current = SortComparatorUtil.calculateCurrent(timeSeries);

        // Assert
        assertEquals("Current should be the last value", 4.0, current, 0.0);
    }

    public void testCalculateCurrentWithEmptySamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("empty", new ArrayList<>());

        // Act
        double current = SortComparatorUtil.calculateCurrent(timeSeries);

        // Assert
        assertEquals("Current of empty series should be 0.0", 0.0, current, 0.0);
    }

    public void testCalculateCurrentWithTrailingNaN() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, Double.NaN));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double current = SortComparatorUtil.calculateCurrent(timeSeries);

        // Assert: Should return the last non-NaN value
        assertEquals("Current should skip trailing NaN", 2.0, current, 0.0);
    }

    public void testCalculateCurrentWithTrailingNull() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 5.0), null);
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double current = SortComparatorUtil.calculateCurrent(timeSeries);

        // Assert: Should return the last non-null value
        assertEquals("Current should skip trailing null", 5.0, current, 0.0);
    }

    public void testCalculateCurrentWithAllInvalidSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null, new FloatSample(3000L, Double.NaN));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double current = SortComparatorUtil.calculateCurrent(timeSeries);

        // Assert: Should return 0.0 when no valid samples
        assertEquals("Current should be 0.0 when all samples are invalid", 0.0, current, 0.0);
    }

    // ========== calculateMax() Tests ==========

    public void testCalculateMaxWithValidSamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(3.0, 1.0, 4.0, 2.0));

        // Act
        double max = SortComparatorUtil.calculateMax(timeSeries);

        // Assert
        assertEquals("Max should be 4.0", 4.0, max, 0.0);
    }

    public void testCalculateMaxWithEmptySamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("empty", new ArrayList<>());

        // Act
        double max = SortComparatorUtil.calculateMax(timeSeries);

        // Assert: When empty, returns Double.NEGATIVE_INFINITY which gets converted to 0.0
        assertEquals("Max of empty series should be Double.NEGATIVE_INFINITY", Double.NEGATIVE_INFINITY, max, 0.0);
    }

    public void testCalculateMaxWithNaNSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 3.0));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double max = SortComparatorUtil.calculateMax(timeSeries);

        // Assert: Should ignore NaN and return max of valid values
        assertEquals("Max should ignore NaN values", 3.0, max, 0.0);
    }

    public void testCalculateMaxWithAllInvalidSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null, new FloatSample(3000L, Double.NaN));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double max = SortComparatorUtil.calculateMax(timeSeries);

        // Assert: Should return 0.0 when no valid samples found
        assertEquals("Max should be 0.0 when all samples are invalid", 0.0, max, 0.0);
    }

    // ========== calculateMin() Tests ==========

    public void testCalculateMinWithValidSamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(3.0, 1.0, 4.0, 2.0));

        // Act
        double min = SortComparatorUtil.calculateMin(timeSeries);

        // Assert
        assertEquals("Min should be 1.0", 1.0, min, 0.0);
    }

    public void testCalculateMinWithEmptySamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("empty", new ArrayList<>());

        // Act
        double min = SortComparatorUtil.calculateMin(timeSeries);

        // Assert
        assertEquals("Min of empty series should be 0.0", 0.0, min, 0.0);
    }

    public void testCalculateMinWithNaNSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 5.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 2.0));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double min = SortComparatorUtil.calculateMin(timeSeries);

        // Assert: Should ignore NaN and return min of valid values
        assertEquals("Min should ignore NaN values", 2.0, min, 0.0);
    }

    public void testCalculateMinWithAllInvalidSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null, new FloatSample(3000L, Double.NaN));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double min = SortComparatorUtil.calculateMin(timeSeries);

        // Assert: Should return 0.0 when no valid samples found
        assertEquals("Min should be 0.0 when all samples are invalid", 0.0, min, 0.0);
    }

    // ========== calculateSum() Tests ==========

    public void testCalculateSumWithValidSamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(1.0, 2.0, 3.0));

        // Act
        double sum = SortComparatorUtil.calculateSum(timeSeries);

        // Assert
        assertEquals("Sum should be 6.0", 6.0, sum, 0.001);
    }

    public void testCalculateSumWithEmptySamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("empty", new ArrayList<>());

        // Act
        double sum = SortComparatorUtil.calculateSum(timeSeries);

        // Assert
        assertEquals("Sum of empty series should be 0.0", 0.0, sum, 0.0);
    }

    public void testCalculateSumWithNaNSamples() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 3.0));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double sum = SortComparatorUtil.calculateSum(timeSeries);

        // Assert: Should ignore NaN and sum valid values
        assertEquals("Sum should ignore NaN values", 4.0, sum, 0.001);
    }

    // ========== calculateStddev() Tests ==========

    public void testCalculateStddevWithValidSamples() {
        // Arrange: Using samples [1.0, 5.0] where avg=3.0, variance=8.0, stddev=2.828...
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(1.0, 5.0));

        // Act
        double stddev = SortComparatorUtil.calculateStddev(timeSeries);

        // Assert: stddev = sqrt(((1-3)^2 + (5-3)^2) / (2-1)) = sqrt(8) ≈ 2.828
        assertEquals("Stddev should be approximately 2.828", 2.828, stddev, 0.01);
    }

    public void testCalculateStddevWithEmptySamples() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("empty", new ArrayList<>());

        // Act
        double stddev = SortComparatorUtil.calculateStddev(timeSeries);

        // Assert
        assertEquals("Stddev of empty series should be 0.0", 0.0, stddev, 0.0);
    }

    public void testCalculateStddevWithSingleSample() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(5.0));

        // Act
        double stddev = SortComparatorUtil.calculateStddev(timeSeries);

        // Assert: Single value should have stddev=0.0
        assertEquals("Stddev of single sample should be 0.0", 0.0, stddev, 0.0);
    }

    public void testCalculateStddevWithIdenticalValues() {
        // Arrange
        TimeSeries timeSeries = createLabeledTimeSeries("test", Arrays.asList(3.0, 3.0, 3.0));

        // Act
        double stddev = SortComparatorUtil.calculateStddev(timeSeries);

        // Assert: Identical values should have stddev=0.0
        assertEquals("Stddev of identical values should be 0.0", 0.0, stddev, 0.0);
    }

    public void testCalculateStddevWithNaNSamples() {
        // Arrange: [2.0, NaN, 6.0] -> valid samples [2.0, 6.0], avg=4.0, stddev=sqrt(8) ≈ 2.828
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 2.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 6.0));
        TimeSeries timeSeries = createTimeSeriesWithSamples("test", samples);

        // Act
        double stddev = SortComparatorUtil.calculateStddev(timeSeries);

        // Assert: Should ignore NaN and calculate stddev of valid values
        assertEquals("Stddev should ignore NaN values", 2.828, stddev, 0.01);
    }

    // ========== extractAlias() Tests ==========

    public void testExtractAliasWithValidAlias() {
        // Arrange
        TimeSeries timeSeries = createTimeSeriesWithAlias("testAlias", Arrays.asList(1.0));

        // Act
        String alias = SortComparatorUtil.extractAlias(timeSeries);

        // Assert
        assertEquals("Should return the alias", "testAlias", alias);
    }

    public void testExtractAliasWithNullAlias() {
        // Arrange
        TimeSeries timeSeries = createTimeSeriesWithAlias(null, Arrays.asList(1.0));

        // Act
        String alias = SortComparatorUtil.extractAlias(timeSeries);

        // Assert
        assertEquals("Should return empty string for null alias", "", alias);
    }

    public void testExtractAliasWithEmptyAlias() {
        // Arrange
        TimeSeries timeSeries = createTimeSeriesWithAlias("", Arrays.asList(1.0));

        // Act
        String alias = SortComparatorUtil.extractAlias(timeSeries);

        // Assert
        assertEquals("Should return empty string for empty alias", "", alias);
    }

    // ========== Helper Methods ==========

    /**
     * Creates a time series with a label identifier for testing.
     */
    private TimeSeries createLabeledTimeSeries(String label, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.fromMap(Map.of("label", label));
        long endTime = values.isEmpty() ? 1000L : 1000L + (values.size() - 1) * 1000L;
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, label);
    }

    /**
     * Creates a time series with custom samples and a label identifier.
     */
    private TimeSeries createTimeSeriesWithSamples(String label, List<Sample> samples) {
        Labels labels = ByteLabels.fromMap(Map.of("label", label));
        long endTime = 1000L;

        // Find the last non-null sample to get endTime
        for (int i = samples.size() - 1; i >= 0; i--) {
            Sample sample = samples.get(i);
            if (sample != null) {
                endTime = sample.getTimestamp();
                break;
            }
        }

        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, label);
    }

    /**
     * Creates a time series with an alias for testing NAME sorting.
     */
    private TimeSeries createTimeSeriesWithAlias(String alias, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.emptyLabels();
        long endTime = values.isEmpty() ? 1000L : 1000L + (values.size() - 1) * 1000L;
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, alias);
    }
}
