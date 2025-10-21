/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for TSDB tests.
 */
public final class TSDBTestUtils {

    // Private constructor to prevent instantiation
    private TSDBTestUtils() {}

    /**
     * Create a TSDB document JSON string from a Labels object.
     *
     * @param labels The Labels object
     * @param timestamp Timestamp in milliseconds since epoch
     * @param val The numeric value of the sample
     * @return JSON string in TSDB engine format
     * @throws IOException If JSON building fails
     */
    public static String createSampleJson(Labels labels, long timestamp, double val) throws IOException {
        return createSampleJson(formatLabelsAsSpaceSeparated(labels.toMapView()), timestamp, val);
    }

    /**
     * Create a TSDB document JSON string from a TimeSeriesSample.
     *
     * @param sample The time series sample to convert
     * @return JSON string in TSDB engine format
     * @throws IOException If JSON building fails
     */
    public static String createTSDBDocumentJson(TimeSeriesSample sample) throws IOException {
        return createSampleJson(formatLabelsAsSpaceSeparated(sample.labels()), sample.timestamp().toEpochMilli(), sample.value());
    }

    /**
     * Create a TSDB document JSON string from labels string, timestamp, and value.
     *
     * @param labelsString Space-separated label key-value pairs (e.g., "name http_requests method GET")
     * @param timestamp Timestamp in milliseconds since epoch
     * @param val The numeric value of the sample
     * @return JSON string in TSDB engine format
     * @throws IOException If JSON building fails
     */
    public static String createSampleJson(String labelsString, long timestamp, double val) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, val);
            builder.endObject();
            return builder.toString();
        }
    }

    /**
     * Format labels as space-separated key-value pairs.
     * Example: {"name": "http_requests", "method": "GET"} -> "name http_requests method GET"
     *
     * @param labels Map of label key-value pairs
     * @return Space-separated string of key-value pairs
     */
    private static String formatLabelsAsSpaceSeparated(Map<String, String> labels) {
        if (labels == null || labels.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            if (!first) {
                sb.append(" ");
            }
            first = false;
            // Format as "key value" pairs separated by spaces
            sb.append(String.format(Locale.ROOT, "%s %s", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }
}
