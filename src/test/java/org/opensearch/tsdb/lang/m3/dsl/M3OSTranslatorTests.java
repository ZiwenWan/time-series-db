/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.dsl;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ParserTests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

public class M3OSTranslatorTests extends OpenSearchTestCase {

    private static final long START_TIME = 1_000_000_000;
    private static final long END_TIME = START_TIME + 1_000_000;
    private static final long STEP = 100_000;

    private final String testCaseName;
    private final String query;
    private final String expected;

    public M3OSTranslatorTests(M3ParserTests.TestCaseData testData) {
        this.testCaseName = testData.name;
        this.query = testData.query;
        this.expected = testData.expected;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        try {
            List<Object[]> testCases = new ArrayList<>();

            NavigableMap<String, String> queries = TestUtils.getResourceFilesWithExtension("lang/m3/data/queries", ".m3ql");
            NavigableMap<String, String> expectedDsl = TestUtils.getResourceFilesWithExtension("lang/m3/data/dsl", ".dsl");

            if (queries.size() != expectedDsl.size()) {
                throw new IllegalStateException("Number of query files does not match result files");
            }

            Iterator<String> queryKeys = queries.keySet().iterator();
            Iterator<String> queryIterator = queries.values().iterator();
            Iterator<String> dslIterator = expectedDsl.values().iterator();

            while (queryIterator.hasNext()) {
                String testCaseName = queryKeys.next();
                String query = queryIterator.next();
                String dsl = dslIterator.next().trim(); // remove trailing newline, which is added by format rules
                testCases.add(new Object[] { new M3ParserTests.TestCaseData(testCaseName, query, dsl) });
            }

            return testCases;
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to load test data: " + e.getMessage(), e);
        }
    }

    public void testM3OSTranslator() {
        try {
            SearchSourceBuilder searchSourceBuilder = M3OSTranslator.translate(
                query,
                new M3OSTranslator.Params(Constants.Time.DEFAULT_TIME_UNIT, START_TIME, END_TIME, STEP, true, false, null, false)
            );
            // Use String comparison to ensure formatting remains consistent
            assertEquals("DSL does not match for test case: " + testCaseName, expected, prettyPrint(searchSourceBuilder.toString()));
        } catch (IOException e) {
            // Handle parsing errors (e.g., if one of the strings is not valid JSON)
            fail("Error parsing JSON string: " + e.getMessage() + testCaseName);
        } catch (Exception e) {
            fail("Failed to run translator test for test case " + testCaseName + ": " + query + " with error: " + e.getMessage());
        }
    }

    private String prettyPrint(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.prettyPrint();
                builder.copyCurrentStructure(parser);
                return builder.toString();
            }
        }
    }
}
