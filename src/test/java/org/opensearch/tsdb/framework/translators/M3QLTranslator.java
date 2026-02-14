/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.framework.models.TimeConfig;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;

import java.time.Instant;

/**
 * Translator for M3QL queries.
 * Converts M3QL query strings to OpenSearch SearchRequest using the M3OSTranslator.
 * Used in both REST tests and internal cluster tests.
 *
 * <p>Supports Cross-Cluster Search (CCS) index patterns and ccs_minimize_roundtrips setting.
 */
public class M3QLTranslator implements QueryConfigTranslator {

    @Override
    public SearchRequest translate(QueryConfig queryConfig, String indices) throws Exception {
        String queryString = queryConfig.query();
        TimeConfig config = queryConfig.config();

        if (config == null) {
            throw new IllegalArgumentException("Query config is required for M3QL query execution");
        }

        // Use already-parsed timestamps and duration from config
        Instant minTime = config.minTimestamp();
        Instant maxTime = config.maxTimestamp();
        long step = config.step().toMillis();

        // Get pushdown setting from query config (default is pushdown enabled, so disablePushdown=false means pushdown=true)
        boolean pushdown = !queryConfig.isDisablePushdown();

        // Get streaming setting from query config (default is disabled)
        boolean streaming = queryConfig.isStreaming();

        // Create M3OSTranslator parameters (no federation metadata for tests)
        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            minTime.toEpochMilli(),
            maxTime.toEpochMilli(),
            step,
            pushdown,
            false,
            null,     // federationMetadata
            streaming
        );

        // Translate M3QL query string to SearchSourceBuilder
        SearchSourceBuilder searchSource = M3OSTranslator.translate(queryString, params);

        // Create SearchRequest with parsed indices (supports CCS patterns)
        String[] indexArray = parseIndices(indices);
        SearchRequest searchRequest = new SearchRequest(indexArray);
        searchRequest.source(searchSource);

        // Apply CCS minimize roundtrips setting if this is a cross-cluster query
        if (queryConfig.isCrossClusterQuery()) {
            searchRequest.setCcsMinimizeRoundtrips(queryConfig.isCcsMinimizeRoundtrips());
        }

        return searchRequest;
    }
}
