/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.opensearch.tsdb.framework.utils.LabelsDeserializer;
import org.opensearch.tsdb.framework.utils.TimestampDeserializer;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Generic metric data with explicit timestamp-value pairs
 *
 * Labels must be specified in statsd format: "__name__:http_requests_total,method:GET,status:200"
 */
public record MetricData(@JsonProperty("labels") @JsonDeserialize(using = LabelsDeserializer.class) Map<String, String> labels,
    @JsonProperty("data_points") List<DataPoint> dataPoints) {

    /**
     * A single data point with timestamp and value
     * For generic data, missing data is represented by absence of a data point,
     * not by null values. Each data point should have both timestamp and value.
     * Timestamps are parsed once during deserialization to ensure consistency.
     */
    public record DataPoint(@JsonProperty("timestamp") @JsonDeserialize(using = TimestampDeserializer.class) Instant timestamp,
        @JsonProperty("value") double value) {
    }
}
