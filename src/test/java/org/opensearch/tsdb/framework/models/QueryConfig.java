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

import org.opensearch.tsdb.framework.translators.QueryType;
import org.opensearch.tsdb.query.rest.ResolvedPartitions;

/**
 * Query configuration for time series testing.
 *
 * <p>Uses the {@link ResolvedPartitions} class with a custom deserializer.
 *
 * <p>For cross-cluster search (CCS) queries, the {@code indices} field can contain
 * cluster-qualified index patterns (e.g., "cluster_a:metrics,cluster_b:metrics").
 * The optional {@code ccs_minimize_roundtrips} field controls CCS optimization behavior.
 *
 * <h3>CCS Query Example:</h3>
 * <pre>{@code
 * queries:
 *   - name: "cross_cluster_sum"
 *     type: "m3ql"
 *     query: "fetch __name__:http_requests | sumSeries region"
 *     indices: "cluster_a:metrics,cluster_b:metrics"
 *     ccs_minimize_roundtrips: true
 *     time_config:
 *       min_timestamp: "now-1h"
 *       max_timestamp: "now"
 *       step: "10m"
 * }</pre>
 *
 * @param name Query name for identification
 * @param type Query type (M3QL or PromQL)
 * @param query The query string
 * @param config Time configuration
 * @param indices Target indices (comma-separated, may include cluster prefixes)
 * @param disablePushdown Optional flag to disable query pushdown
 * @param streaming Optional flag to enable streaming aggregation
 * @param ccsMinimizeRoundtrips Optional CCS minimize roundtrips setting (default: true)
 * @param resolvedPartitions Optional pre-resolved partitions
 * @param expected Expected response for validation
 */
public record QueryConfig(@JsonProperty("name") String name, @JsonProperty("type") QueryType type, @JsonProperty("query") String query,
    @JsonProperty("time_config") TimeConfig config, @JsonProperty("indices") String indices,
    @JsonProperty("disable_pushdown") Boolean disablePushdown, @JsonProperty("streaming") Boolean streaming,
    @JsonProperty("ccs_minimize_roundtrips") Boolean ccsMinimizeRoundtrips,
    @JsonProperty("resolved_partitions") @JsonDeserialize(using = ResolvedPartitionsYamlAdapter.Deserializer.class) ResolvedPartitions resolvedPartitions,
    @JsonProperty("expected") ExpectedResponse expected) {

    /**
     * Get the disable pushdown flag, defaulting to false if not specified.
     */
    public boolean isDisablePushdown() {
        return disablePushdown != null && disablePushdown;
    }

    /**
     * Get the streaming flag, defaulting to false if not specified.
     */
    public boolean isStreaming() {
        return streaming != null && streaming;
    }

    /**
     * Get the CCS minimize roundtrips setting, defaulting to true if not specified.
     */
    public boolean isCcsMinimizeRoundtrips() {
        return ccsMinimizeRoundtrips == null || ccsMinimizeRoundtrips;
    }

    /**
     * Returns true if this query targets multiple clusters (CCS query).
     */
    public boolean isCrossClusterQuery() {
        return indices != null && indices.contains(":");
    }
}
