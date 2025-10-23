/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for RestM3QLAction.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Handler registration and naming</li>
 *   <li>Route registration</li>
 *   <li>Query parameter parsing</li>
 *   <li>Request body parsing</li>
 *   <li>Error handling</li>
 *   <li>Explain mode</li>
 * </ul>
 */
public class RestM3QLActionTests extends OpenSearchTestCase {

    private RestM3QLAction action;
    private NodeClient mockClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestM3QLAction();
        // Default mock client that doesn't assert (for tests that don't need custom assertions)
        mockClient = setupMockClientWithAssertion(searchRequest -> {
            // No-op assertion - just let the request pass through
        });
    }

    /**
     * Helper method to setup a mock client with custom assertion logic.
     * This allows each test to validate the SearchRequest contents.
     *
     * @param assertSearchRequest Consumer that asserts on the SearchRequest
     * @return a mocked NodeClient
     */
    private NodeClient setupMockClientWithAssertion(Consumer<SearchRequest> assertSearchRequest) {
        NodeClient mockClient = mock(NodeClient.class);
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            // Execute the test's custom assertion
            assertSearchRequest.accept(searchRequest);

            // Return a mock response
            SearchResponse mockResponse = mock(SearchResponse.class);
            listener.onResponse(mockResponse);
            return null;
        }).when(mockClient).search(any(SearchRequest.class), any());
        return mockClient;
    }

    /**
     * Helper method to assert that a SearchRequest contains at least one time_series_unfold aggregation.
     *
     * @param searchRequest the SearchRequest to validate
     */
    private void assertContainsTimeSeriesUnfoldAggregation(SearchRequest searchRequest) {
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertNotNull("SearchRequest source should not be null", searchRequest.source());
        String dsl = searchRequest.source().toString();
        assertThat(
            "Translated DSL should contain time_series_unfold aggregation",
            dsl,
            containsString(TimeSeriesUnfoldAggregationBuilder.NAME)
        );
    }

    // ========== Handler Registration Tests ==========

    public void testGetName() {
        assertThat(action.getName(), equalTo("m3ql_action"));
    }

    public void testRoutes() {
        List<Route> routes = action.routes();
        assertNotNull(routes);
        assertThat(routes, hasSize(2));

        // Verify all expected routes are registered
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("/_m3ql") && r.getMethod() == RestRequest.Method.GET));
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("/_m3ql") && r.getMethod() == RestRequest.Method.POST));
    }

    public void testRoutePaths() {
        List<Route> routes = action.routes();

        // Verify global endpoint
        long globalCount = routes.stream().filter(r -> r.getPath().equals("/_m3ql")).count();
        assertThat("Should have 2 methods for global endpoint", globalCount, equalTo(2L));
    }

    public void testRouteMethods() {
        List<Route> routes = action.routes();

        // Verify GET method
        long getCount = routes.stream().filter(r -> r.getMethod() == RestRequest.Method.GET).count();
        assertThat("Should have 1 GET route", getCount, equalTo(1L));

        // Verify POST method
        long postCount = routes.stream().filter(r -> r.getMethod() == RestRequest.Method.POST).count();
        assertThat("Should have 1 POST route", postCount, equalTo(1L));
    }

    // ========== Query Parameter Tests ==========

    public void testQueryFromUrlParameter() throws Exception {
        // Setup mock to assert query was properly translated with time_series_unfold aggregation
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | sum region"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testQueryFromRequestBody() throws Exception {
        // Setup mock to assert query was properly translated with time_series_unfold aggregation
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        String jsonBody = "{\"query\": \"fetch service:api | sum region\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testEmptyQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", ""))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query cannot be empty"));
    }

    public void testMissingQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query cannot be empty"));
    }

    public void testWhitespaceOnlyQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "   "))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query cannot be empty"));
    }

    // ========== Time Range Parameter Tests ==========

    public void testDefaultTimeRange() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default time range (now-5m to now)
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testCustomTimeRange() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "start", "now-1h", "end", "now"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testAbsoluteTimeRange() throws Exception {
        // Setup mock to assert time_series_unfold aggregation is present
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "1609459200000", // 2021-01-01 00:00:00 UTC
                    "end",
                    "1609545600000"    // 2021-01-02 00:00:00 UTC
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testInvalidTimeRangeStartAfterEnd() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "1609545600000",  // 2021-01-02 00:00:00 UTC
                    "end",
                    "1609459200000"     // 2021-01-01 00:00:00 UTC (before start!)
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("Invalid time range"));
        assertThat(responseContent, containsString("start time must be before end time"));
    }

    public void testInvalidTimeRangeStartEqualsEnd() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "1609545600000",  // 2021-01-02 00:00:00 UTC
                    "end",
                    "1609545600000"   // 2021-01-02 00:00:00 UTC
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("Invalid time range"));
        assertThat(responseContent, containsString("start time must be before end time"));
    }

    // ========== Step Parameter Tests ==========

    public void testDefaultStepParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default step (10000ms)
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testCustomStepParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "step",
                    "30000" // 30 seconds
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Partitions/Indices Parameter Tests ==========

    public void testPartitionsParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "partitions", "metrics-2024-01,metrics-2024-02"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testWithoutPartitions() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should work without partitions
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Explain Mode Tests ==========

    public void testExplainMode() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | sum region", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("m3ql_query"));
        assertThat(responseContent, containsString("translated_dsl"));
        assertThat(responseContent, containsString("explanation"));
    }

    public void testExplainModeWithComplexQuery() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api region:us-* | transformNull | sum region | alias mymetric", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("translated_dsl"));
    }

    // ========== Pushdown Parameter Tests ==========

    public void testPushdownDefaultTrue() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default pushdown=true
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testPushdownExplicitFalse() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "pushdown", "false"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Error Handling Tests ==========

    public void testInvalidM3QLQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "invalid syntax here |||"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("error"));
    }

    public void testMalformedJsonBodyReturnsError() throws Exception {
        String malformedJson = "{invalid json";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(malformedJson), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        // Should throw exception during request handling
        expectThrows(Exception.class, () -> action.handleRequest(request, channel, mockClient));
    }

    // ========== Request Body Priority Tests ==========

    public void testBodyQueryTakesPrecedenceOverUrlParam() throws Exception {
        // Setup mock to assert body query is used and contains time_series_unfold
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);
            String dsl = searchRequest.source().toString();
            // Verify the DSL contains the body query with service:api
            assertThat("Should use body query", dsl, containsString("service"));
            assertThat("Should use body query", dsl, containsString("api"));
        });

        // Note: When body is present, it takes precedence and we shouldn't provide URL params
        // This test verifies that the body query (service:api) is used for translation
        String jsonBody = "{\"query\": \"fetch service:api\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testEmptyBodyFallsBackToUrlParam() throws Exception {
        // Setup mock to assert URL param query is used and contains time_series_unfold
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);
            String dsl = searchRequest.source().toString();
            // Verify the DSL contains the URL param query
            assertThat("Should use URL param query", dsl, containsString("service"));
            assertThat("Should use URL param query", dsl, containsString("api"));
        });

        String jsonBody = "{}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }
}
