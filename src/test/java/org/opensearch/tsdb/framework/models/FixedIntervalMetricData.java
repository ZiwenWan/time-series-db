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

import java.util.Map;

/**
 * Fixed interval metric data with fixed-interval timestamps
 * Used when time_config is specified with min_timestamp, max_timestamp, and step.
 * Null values in the array represent missing data points (blips) at specific timestamps.
 *
 * Labels must be specified in statsd format: "__name__:http_requests_total,method:GET,status:200"
 */
public record FixedIntervalMetricData(@JsonProperty("labels") @JsonDeserialize(using = LabelsDeserializer.class) Map<String, String> labels,
    @JsonProperty("values") Double[] values) {
}
