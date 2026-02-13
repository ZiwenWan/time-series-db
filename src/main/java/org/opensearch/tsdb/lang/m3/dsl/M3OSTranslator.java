/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.dsl;

import org.opensearch.ExceptionsHelper;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.ParseException;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3ASTConverter;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.optimizer.M3PlanOptimizer;
import org.opensearch.tsdb.query.federation.FederationMetadata;

import java.util.concurrent.TimeUnit;

/**
 * M3OSTranslator is responsible for translating M3QL AST to OpenSearch DSL.
 */
public class M3OSTranslator {

    private M3OSTranslator() {}

    /**
     * Creates a SearchSourceBuilder from the given M3QL query string.
     *
     * @param query The M3QL query string
     * @param params Query parameters
     * @return SearchSourceBuilder ready for execution
     */
    public static SearchSourceBuilder translate(String query, Params params) {
        try (M3PlannerContext context = M3PlannerContext.create()) {
            return translateInternal(query, context, params);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        } catch (Throwable t) {
            // catch any Error thrown by the javacc generated parser, to prevent crashing
            throw new RuntimeException(t);
        }
    }

    private static SearchSourceBuilder translateInternal(String query, M3PlannerContext context, Params params) throws ParseException {
        // 1. Parse query and create AST
        RootNode astRoot = M3QLParser.parse(query, true);

        // 2. Convert AST to unoptimized plan
        M3ASTConverter astConverter = new M3ASTConverter(context);
        M3PlanNode planRoot = astConverter.buildPlan(astRoot);

        // 3. Apply optimizations to the plan
        planRoot = M3PlanOptimizer.optimize(planRoot);

        // 4. Convert to SearchSourceBuilder
        // turn off track_total_hits as we don't need it for TSDB queries and it can take up a lot of CPU time
        return new SourceBuilderVisitor(params).process(planRoot).toSearchSourceBuilder().profile(params.profile()).trackTotalHits(false);
    }

    /**
     * Query params used during query construction.
     * @param timeUnit Time unit for startTime and endTime
     * @param startTime Query start time in the specified time unit
     * @param endTime Query end time in the specified time unit
     * @param step Step interval for aggregations in the specified time unit
     * @param pushdown Enable pushdown optimizations
     * @param profile Enable profiling
     * @param federationMetadata Metadata about federated query execution across partitions (nullable)
     * @param streaming Enable streaming aggregation optimization for eligible queries
     */
    public record Params(TimeUnit timeUnit, long startTime, long endTime, long step, boolean pushdown, boolean profile,
        FederationMetadata federationMetadata, boolean streaming) {

        /**
         * Validation for params.
         */
        public Params {
            if (startTime >= endTime) {
                throw new IllegalArgumentException("Start time must be less than end time");
            }
        }

        /**
         * Constructor with default time unit and streaming disabled.
         * @param startTime Query start time in default time unit
         * @param endTime Query end time in default time unit
         * @param step Step interval for aggregations in default time unit
         * @param pushdown Enable pushdown optimizations
         * @param profile Enable profiling
         * @param federationMetadata Metadata about federated query execution across partitions (nullable)
         */
        public Params(long startTime, long endTime, long step, boolean pushdown, boolean profile, FederationMetadata federationMetadata) {
            this(Constants.Time.DEFAULT_TIME_UNIT, startTime, endTime, step, pushdown, profile, federationMetadata, false);
        }
    }
}
