/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.plugins.Plugin;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.TimeSeriesTestFramework;

import java.util.Collection;
import java.util.List;

/**
 * Integration tests for streaming aggregation.
 * Validates that streaming aggregation produces identical results to unfold aggregation
 * for eligible queries (fetch | sum/min/max/avg).
 */
public class StreamingAggregationIT extends TimeSeriesTestFramework {

    private static final String STREAMING_TEST_YAML = "test_cases/streaming_aggregation_it.yaml";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Test streaming aggregation correctness for sum, min, max, avg.
     * Verifies that streaming results match unfold results.
     */
    public void testStreamingAggregation() throws Exception {
        loadTestConfigurationFromFile(STREAMING_TEST_YAML);
        runBasicTest();
    }
}
