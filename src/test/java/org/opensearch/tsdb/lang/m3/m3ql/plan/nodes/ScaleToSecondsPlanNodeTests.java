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

/**
 * Unit tests for ScaleToSecondsPlanNode.
 */
public class ScaleToSecondsPlanNodeTests extends BasePlanNodeTests {

    public void testScaleToSecondsPlanNodeCreation() {
        ScaleToSecondsPlanNode node = new ScaleToSecondsPlanNode(1, 10);

        assertEquals(1, node.getId());
        assertEquals(10, node.getSeconds());
        assertEquals("SCALE_TO_SECONDS(10)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testScaleToSecondsPlanNodeWithLargeSeconds() {
        ScaleToSecondsPlanNode node = new ScaleToSecondsPlanNode(1, 3600);

        assertEquals(3600, node.getSeconds());
        assertEquals("SCALE_TO_SECONDS(3600)", node.getExplainName());
    }

    public void testScaleToSecondsPlanNodeVisitorAccept() {
        ScaleToSecondsPlanNode node = new ScaleToSecondsPlanNode(1, 5);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit ScaleToSecondsPlanNode", result);
    }

    public void testScaleToSecondsPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scaleToSeconds");
        functionNode.addChildNode(new ValueNode("2"));

        ScaleToSecondsPlanNode node = ScaleToSecondsPlanNode.of(functionNode);

        assertEquals(2, node.getSeconds());
    }

    public void testScaleToSecondsPlanNodeFactoryMethodWithLargeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scaleToSeconds");
        functionNode.addChildNode(new ValueNode("60"));

        ScaleToSecondsPlanNode node = ScaleToSecondsPlanNode.of(functionNode);

        assertEquals(60, node.getSeconds());
    }

    public void testScaleToSecondsPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scaleToSeconds");

        expectThrows(IllegalArgumentException.class, () -> ScaleToSecondsPlanNode.of(functionNode));
    }

    public void testScaleToSecondsPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scaleToSeconds");
        functionNode.addChildNode(new ValueNode("2"));
        functionNode.addChildNode(new ValueNode("3"));

        expectThrows(IllegalArgumentException.class, () -> ScaleToSecondsPlanNode.of(functionNode));
    }

    public void testScaleToSecondsPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scaleToSeconds");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> ScaleToSecondsPlanNode.of(functionNode));
    }

    public void testScaleToSecondsPlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scaleToSeconds");
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(NumberFormatException.class, () -> ScaleToSecondsPlanNode.of(functionNode));
    }

    public void testScaleToSecondsPlanNodeChildrenManagement() {
        ScaleToSecondsPlanNode parentNode = new ScaleToSecondsPlanNode(1, 5);
        ScaleToSecondsPlanNode childNode = new ScaleToSecondsPlanNode(2, 10);

        assertTrue(parentNode.getChildren().isEmpty());

        parentNode.addChild(childNode);

        assertEquals(1, parentNode.getChildren().size());
        assertEquals(childNode, parentNode.getChildren().getFirst());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(ScaleToSecondsPlanNode planNode) {
            return "visit ScaleToSecondsPlanNode";
        }
    }
}
