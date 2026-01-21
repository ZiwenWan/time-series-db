/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.util;

import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.Comparator;
import java.util.List;

/**
 * Utility class for creating comparators used by sort-related stages.
 * This class provides shared logic for comparing time series based on various criteria.
 *
 * Used by SortStage and TopKStage to maintain consistent sorting behavior.
 */
public final class SortComparatorUtil {

    private SortComparatorUtil() {
        // Utility class - prevent instantiation
    }

    /**
     * Create a comparator that compares time series based on the specified sort criteria.
     *
     * @param sortBy the criteria to sort by (avg, current, max, min, sum, stddev, name)
     * @return a comparator for TimeSeries objects
     */
    public static Comparator<TimeSeries> createComparator(SortByType sortBy) {
        return switch (sortBy) {
            case AVG -> Comparator.comparingDouble(SortComparatorUtil::calculateAverage);
            case CURRENT -> Comparator.comparingDouble(SortComparatorUtil::calculateCurrent);
            case MAX -> Comparator.comparingDouble(SortComparatorUtil::calculateMax);
            case MIN -> Comparator.comparingDouble(SortComparatorUtil::calculateMin);
            case SUM -> Comparator.comparingDouble(SortComparatorUtil::calculateSum);
            case STDDEV -> Comparator.comparingDouble(SortComparatorUtil::calculateStddev);
            case NAME -> Comparator.comparing(SortComparatorUtil::extractAlias);
        };
    }

    /**
     * Calculate the average of all values in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the average for
     * @return the average value, or 0.0 if no valid samples
     */
    public static double calculateAverage(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        int count = 0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
                count++;
            }
        }

        return count == 0 ? 0.0 : sum / count;
    }

    /**
     * Calculate the last (current) value in the time series as the sorting key.
     *
     * @param timeSeries the time series to get the current value from
     * @return the last non-NaN value, or 0.0 if no valid samples
     */
    public static double calculateCurrent(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }
        for (int i = samples.size() - 1; i >= 0; i--) {
            Sample sample = samples.get(i);
            if (sample != null && !Double.isNaN(sample.getValue())) {
                return sample.getValue();
            }
        }
        return 0.0;
    }

    /**
     * Calculate the maximum value in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the maximum for
     * @return the maximum value, or 0.0 if no valid samples
     */
    public static double calculateMax(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NEGATIVE_INFINITY;
        }

        double max = Double.NEGATIVE_INFINITY;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                max = Math.max(max, sample.getValue());
            }
        }

        return max == Double.NEGATIVE_INFINITY ? 0.0 : max;
    }

    /**
     * Calculate the minimum value in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the minimum for
     * @return the minimum value, or 0.0 if no valid samples
     */
    public static double calculateMin(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double min = Double.POSITIVE_INFINITY;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                min = Math.min(min, sample.getValue());
            }
        }

        return min == Double.POSITIVE_INFINITY ? 0.0 : min;
    }

    /**
     * Calculate the sum of all values in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the sum for
     * @return the sum of all values, or 0.0 if no valid samples
     */
    public static double calculateSum(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
            }
        }

        return sum;
    }

    /**
     * Calculate the standard deviation of all values in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the standard deviation for
     * @return the standard deviation, or 0.0 if insufficient samples
     */
    public static double calculateStddev(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        // Count valid (non-NaN, non-null) samples
        long validCount = samples.stream().filter(s -> s != null && !Double.isNaN(s.getValue())).count();

        if (validCount <= 1) {
            return 0.0;
        }

        double avg = calculateAverage(timeSeries);
        double sumOfSquaredDifferences = samples.stream()
            .filter(s -> s != null && !Double.isNaN(s.getValue()))
            .map(s -> Math.pow(s.getValue() - avg, 2))
            .mapToDouble(Double::doubleValue)
            .sum();
        double variance = sumOfSquaredDifferences / (validCount - 1);
        return Math.sqrt(variance);
    }

    /**
     * Extract the alias from the time series labels as the sorting key.
     * null alias will be treated as empty string.
     *
     * @param timeSeries The time series to extract the alias from
     * @return The value of the alias, or empty string if null
     */
    public static String extractAlias(TimeSeries timeSeries) {
        return timeSeries.getAlias() == null ? "" : timeSeries.getAlias();
    }
}
