/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.time.FormatNames;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;
import org.opensearch.tsdb.query.utils.AggregationNameExtractor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for M3QL queries.
 *
 * <p>This handler translates M3QL queries to OpenSearch DSL and executes them,
 * returning results in Prometheus matrix format. It provides a simple interface
 * for executing M3QL queries over time series data stored in OpenSearch.</p>
 *
 * <h2>Supported Routes:</h2>
 * <ul>
 *   <li>GET/POST /_m3ql - Execute M3QL query (use 'partitions' param for specific indices)</li>
 * </ul>
 *
 * <h2>Request Parameters:</h2>
 * <ul>
 *   <li><b>query</b> (required): M3QL query string (can be in body or URL param)</li>
 *   <li><b>start</b> (optional): Start time (default: "now-5m")</li>
 *   <li><b>end</b> (optional): End time (default: "now")</li>
 *   <li><b>step</b> (optional): Step interval in milliseconds (default: 10000)</li>
 *   <li><b>partitions</b> (optional): Comma-separated list of indices to query</li>
 *   <li><b>explain</b> (optional): Return translated DSL instead of executing (default: false)</li>
 *   <li><b>pushdown</b> (optional): Enable pushdown optimizations (default: true)</li>
 * </ul>
 *
 * <h2>Request Body (JSON):</h2>
 * <pre>{@code
 * {
 *   "query": "fetch service:api | sum region"
 * }
 * }</pre>
 *
 * <h2>Response Format:</h2>
 * <p>Returns Prometheus matrix format (see {@link PromMatrixResponseListener})</p>
 *
 * @see M3OSTranslator
 * @see PromMatrixResponseListener
 */
public class RestM3QLAction extends BaseRestHandler {

    public static final String NAME = "m3ql_action";

    // Route path
    private static final String BASE_M3QL_PATH = "/_m3ql";

    // Request parameter names
    private static final String QUERY_PARAM = "query";
    private static final String START_PARAM = "start";
    private static final String END_PARAM = "end";
    private static final String STEP_PARAM = "step";
    private static final String PARTITIONS_PARAM = "partitions";
    private static final String EXPLAIN_PARAM = "explain";
    private static final String PUSHDOWN_PARAM = "pushdown";

    // Default parameter values
    private static final String DEFAULT_START_TIME = "now-5m";
    private static final String DEFAULT_END_TIME = "now";
    private static final long DEFAULT_STEP_MS = 10_000L; // 10 seconds

    // Response field names
    private static final String ERROR_FIELD = "error";
    private static final String M3QL_QUERY_FIELD = "m3ql_query";
    private static final String TRANSLATED_DSL_FIELD = "translated_dsl";
    private static final String EXPLANATION_FIELD = "explanation";

    // Date format pattern
    private static final String DATE_FORMAT_PATTERN = FormatNames.STRICT_DATE_OPTIONAL_TIME.getSnakeCaseName()
        + "||"
        + FormatNames.EPOCH_MILLIS.getSnakeCaseName();

    // Date parser for consistent time parsing across OpenSearch
    private static final DateMathParser DATE_MATH_PARSER = DateFormatter.forPattern(DATE_FORMAT_PATTERN).toDateMathParser();

    /**
     * Constructs a new RestM3QLAction handler.
     */
    public RestM3QLAction() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_M3QL_PATH), new Route(POST, BASE_M3QL_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse and validate request parameters
        final RequestParams params;
        try {
            params = parseRequestParams(request);
        } catch (IllegalArgumentException e) {
            return channel -> {
                XContentBuilder response = channel.newErrorBuilder();
                response.startObject();
                response.field(ERROR_FIELD, e.getMessage());
                response.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
            };
        }

        // Validate query
        if (params.query == null || params.query.trim().isEmpty()) {
            return channel -> {
                XContentBuilder response = channel.newErrorBuilder();
                response.startObject();
                response.field(ERROR_FIELD, "Query cannot be empty");
                response.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
            };
        }

        // Translate M3QL to OpenSearch DSL
        try {
            final SearchSourceBuilder searchSourceBuilder = translateQuery(params);

            // Handle explain mode
            if (params.explain) {
                return buildExplainResponse(params.query, searchSourceBuilder);
            }

            // Build and execute search request
            final SearchRequest searchRequest = buildSearchRequest(params, searchSourceBuilder);
            final String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);

            return channel -> client.search(searchRequest, new PromMatrixResponseListener(channel, finalAggName));

        } catch (Exception e) {
            return channel -> {
                XContentBuilder response = channel.newErrorBuilder();
                response.startObject();
                response.field(ERROR_FIELD, e.getMessage());
                response.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
            };
        }
    }

    /**
     * Parses request parameters from the REST request.
     *
     * @param request the REST request
     * @return parsed request parameters
     * @throws IOException if parsing fails
     */
    private RequestParams parseRequestParams(RestRequest request) throws IOException {
        // Extract M3QL query from body or URL parameter
        String query = extractM3QLQuery(request);

        // Capture base time once for consistent "now" across all time parameters in this request
        long nowMillis = System.currentTimeMillis();

        // Parse time range (default to last 5 minutes)
        long startMs = parseTimeParam(request, START_PARAM, DEFAULT_START_TIME, nowMillis);
        long endMs = parseTimeParam(request, END_PARAM, DEFAULT_END_TIME, nowMillis);

        // Validate time range: start must be before end
        if (startMs >= endMs) {
            throw new IllegalArgumentException(
                "Invalid time range: start time must be before end time (start=" + startMs + ", end=" + endMs + ")"
            );
        }

        // Parse step interval
        long stepMs = request.paramAsLong(STEP_PARAM, DEFAULT_STEP_MS);

        // Parse indices/partitions
        String[] indices = Strings.splitStringByCommaToArray(request.param(PARTITIONS_PARAM));

        // Parse flags
        boolean explain = request.paramAsBoolean(EXPLAIN_PARAM, false);
        boolean pushdown = request.paramAsBoolean(PUSHDOWN_PARAM, true);

        return new RequestParams(query, startMs, endMs, stepMs, indices, explain, pushdown);
    }

    /**
     * Parses a time parameter from the request and converts it to milliseconds since epoch.
     *
     * @param request the REST request
     * @param paramName the name of the time parameter
     * @param defaultValue the default value if parameter is not provided
     * @param nowMillis the base time in milliseconds to use for "now" references
     * @return the parsed time in milliseconds since epoch
     */
    private long parseTimeParam(RestRequest request, String paramName, String defaultValue, long nowMillis) {
        String timeString = request.param(paramName, defaultValue);
        Instant instant = DATE_MATH_PARSER.parse(timeString, () -> nowMillis);
        return instant.toEpochMilli();
    }

    /**
     * Extracts the M3QL query from the request body or URL parameter.
     *
     * @param request the REST request
     * @return the M3QL query string, or null if not found
     * @throws IOException if parsing the request body fails
     */
    private String extractM3QLQuery(RestRequest request) throws IOException {
        // Try request body first
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                Map<String, Object> bodyMap = parser.map();
                Object queryObj = bodyMap.get(QUERY_PARAM);
                if (queryObj != null) {
                    return queryObj.toString();
                }
            }
        }

        // Fall back to URL parameter
        return request.param(QUERY_PARAM);
    }

    /**
     * Translates an M3QL query to OpenSearch DSL.
     *
     * @param params request parameters containing the query and time range
     * @return the translated SearchSourceBuilder
     */
    private SearchSourceBuilder translateQuery(RequestParams params) {
        M3OSTranslator.Params translatorParams = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            params.startMs,
            params.endMs,
            params.stepMs,
            params.pushdown
        );
        return M3OSTranslator.translate(params.query, translatorParams);
    }

    /**
     * Builds a SearchRequest from the translated query.
     *
     * @param params request parameters
     * @param searchSourceBuilder the translated search source
     * @return configured SearchRequest
     */
    private SearchRequest buildSearchRequest(RequestParams params, SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        // Disable query cache explicitly as TSDB MetricsDirectoryReader does not support caching
        // TODO: Remove this once TSDB MetricsDirectoryReader supports caching
        searchRequest.requestCache(false);

        // Set indices if specified
        if (params.indices.length > 0) {
            searchRequest.indices(params.indices);
        }

        return searchRequest;
    }

    /**
     * Builds a response for explain mode that returns the translated DSL.
     *
     * @param query the original M3QL query
     * @param searchSourceBuilder the translated DSL
     * @return a RestChannelConsumer that sends the explain response
     */
    private RestChannelConsumer buildExplainResponse(String query, SearchSourceBuilder searchSourceBuilder) {
        return channel -> {
            XContentBuilder response = channel.newBuilder();
            response.startObject();
            response.field(M3QL_QUERY_FIELD, query);
            response.field(TRANSLATED_DSL_FIELD, searchSourceBuilder.toString());
            response.field(EXPLANATION_FIELD, "M3QL query translated to OpenSearch DSL");
            response.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response));
        };
    }

    /**
     * Internal record holding parsed request parameters.
     */
    private record RequestParams(String query, long startMs, long endMs, long stepMs, String[] indices, boolean explain, boolean pushdown) {
    }
}
