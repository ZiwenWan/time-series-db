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
 * Integration tests for inplace aggregation.
 * Validates that inplace aggregation produces identical results to unfold aggregation
 * for eligible queries (fetch | sum/min/max/avg).
 */
public class InplaceAggregationIT extends TimeSeriesTestFramework {

    private static final String INPLACE_TEST_YAML = "test_cases/inplace_aggregation_it.yaml";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Test inplace aggregation correctness for sum, min, max, avg.
     * Verifies that inplace results match unfold results.
     */
    public void testInplaceAggregation() throws Exception {
        loadTestConfigurationFromFile(INPLACE_TEST_YAML);
        runBasicTest();
    }
}
