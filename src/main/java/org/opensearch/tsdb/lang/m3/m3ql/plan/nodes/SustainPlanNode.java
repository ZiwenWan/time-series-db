/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.util.List;
import java.util.Locale;

/**
 * Plan node for filtering samples based on sustained non-null value windows.
 * Removes individual data points that do not have an uninterrupted prefix of
 * non-null values for the specified duration.
 */
public class SustainPlanNode extends M3PlanNode {

    private final String duration; // e.g., "5m", "10s"

    /**
     * Constructor for SustainPlanNode.
     *
     * @param id       node id
     * @param duration the minimum time duration (e.g., "5m" for 5 minutes, "10s" for 10 seconds)
     */
    public SustainPlanNode(int id, String duration) {
        super(id);
        this.duration = duration;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "SUSTAIN(%s)", duration);
    }

    /**
     * Returns the duration.
     *
     * @return Duration of the sustain window
     * @throws IllegalArgumentException if the duration is negative
     */
    public Duration getDuration() {
        Duration parsedDuration = M3Duration.valueOf(duration);
        if (parsedDuration.isNegative()) {
            throw new IllegalArgumentException("Duration cannot be negative: " + duration);
        }
        return parsedDuration;
    }

    /**
     * Factory method to create a SustainPlanNode from a FunctionNode.
     *
     * @param functionNode the function node representing the SUSTAIN function
     * @return a new SustainPlanNode instance
     */
    public static SustainPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("Sustain function expects exactly one argument (duration)");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Sustain expects a value node as the first argument");
        }

        String duration = valueNode.getValue();
        return new SustainPlanNode(M3PlannerContext.generateId(), duration);
    }
}
