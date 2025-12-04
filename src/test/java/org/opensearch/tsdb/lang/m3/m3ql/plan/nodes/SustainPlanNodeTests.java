/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * Unit tests for SustainPlanNode.
 */
public class SustainPlanNodeTests extends BasePlanNodeTests {

    public void testSustainPlanNodeCreation() {
        SustainPlanNode node = new SustainPlanNode(1, "5s");

        assertEquals(1, node.getId());
        assertEquals(5000, node.getDuration().toMillis());
        assertEquals("SUSTAIN(5s)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testSustainPlanNodeWithZeroDuration() {
        SustainPlanNode node = new SustainPlanNode(1, "0s");

        assertEquals(0, node.getDuration().toMillis());
        assertEquals("SUSTAIN(0s)", node.getExplainName());
    }

    public void testSustainPlanNodeWithNegativeDuration() {
        SustainPlanNode node = new SustainPlanNode(1, "-10s");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> node.getDuration());
        assertTrue(exception.getMessage().contains("negative"));
    }

    public void testSustainPlanNodeVisitorAccept() {
        SustainPlanNode node = new SustainPlanNode(1, "3s");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit SustainPlanNode", result);
    }

    public void testSustainPlanNodeFactoryMethodWithStringDuration() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sustain");
        functionNode.addChildNode(new ValueNode("5m"));

        SustainPlanNode node = SustainPlanNode.of(functionNode);

        assertEquals(300000, node.getDuration().toMillis());
        assertEquals("SUSTAIN(5m)", node.getExplainName());
    }

    public void testSustainPlanNodeFactoryMethodWithMissingArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sustain");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SustainPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("duration") || exception.getMessage().contains("argument"));
    }

    public void testSustainPlanNodeFactoryMethodWithNonValueNodeArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sustain");
        // Add a FunctionNode instead of ValueNode to trigger the type check
        FunctionNode childFunction = new FunctionNode();
        childFunction.setFunctionName("fetch");
        functionNode.addChildNode(childFunction);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SustainPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("value node"));
    }

    public void testSustainPlanNodeFactoryMethodWithInvalidDuration() {
        // Test with non-parseable duration format
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sustain");
        functionNode.addChildNode(new ValueNode("invalid"));

        // Node creation succeeds, but getDuration() throws exception
        SustainPlanNode node = SustainPlanNode.of(functionNode);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> node.getDuration());
        String message = exception.getMessage().toLowerCase(Locale.ROOT);
        assertTrue(message.contains("duration") || message.contains("invalid"));
    }

    public void testSustainPlanNodeFactoryMethodWithVariousDurations() {
        FunctionNode fn1 = new FunctionNode();
        fn1.setFunctionName("sustain");
        fn1.addChildNode(new ValueNode("30s"));
        assertEquals(30000, SustainPlanNode.of(fn1).getDuration().toMillis());

        FunctionNode fn2 = new FunctionNode();
        fn2.setFunctionName("sustain");
        fn2.addChildNode(new ValueNode("5m"));
        assertEquals(300000, SustainPlanNode.of(fn2).getDuration().toMillis());

        FunctionNode fn3 = new FunctionNode();
        fn3.setFunctionName("sustain");
        fn3.addChildNode(new ValueNode("1h"));
        assertEquals(3600000, SustainPlanNode.of(fn3).getDuration().toMillis());

        FunctionNode fn4 = new FunctionNode();
        fn4.setFunctionName("sustain");
        fn4.addChildNode(new ValueNode("2d"));
        assertEquals(172800000, SustainPlanNode.of(fn4).getDuration().toMillis());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(SustainPlanNode planNode) {
            return "visit SustainPlanNode";
        }
    }
}
