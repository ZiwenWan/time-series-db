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

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class AliasByDistinctTagsPlanNodeTests extends BasePlanNodeTests {

    public void testAliasByDistinctTagsPlanNodeCreation() {
        AliasByDistinctTagsPlanNode node = new AliasByDistinctTagsPlanNode(1, true, List.of("env", "dc"));

        assertEquals(1, node.getId());
        assertTrue(node.isIncludeKeys());
        assertEquals(List.of("env", "dc"), node.getTagNames());
        assertEquals("ALIAS_BY_DISTINCT_TAGS(includeKeys=true, tags=env,dc)", node.getExplainName());
    }

    public void testAliasByDistinctTagsPlanNodeAutoDetection() {
        AliasByDistinctTagsPlanNode node = new AliasByDistinctTagsPlanNode(1, false, null);

        assertEquals(1, node.getId());
        assertFalse(node.isIncludeKeys());
        assertNull(node.getTagNames());
        assertEquals("ALIAS_BY_DISTINCT_TAGS(includeKeys=false, tags=auto)", node.getExplainName());
    }

    public void testFactoryMethodBooleanAndTags() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode boolNode = mock(ValueNode.class);
        ValueNode tagNode1 = mock(ValueNode.class);
        ValueNode tagNode2 = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(boolNode, tagNode1, tagNode2));
        when(boolNode.getValue()).thenReturn("\"true\"");
        when(tagNode1.getValue()).thenReturn("\"env\"");
        when(tagNode2.getValue()).thenReturn("\"dc\"");

        AliasByDistinctTagsPlanNode node = AliasByDistinctTagsPlanNode.of(functionNode);

        assertTrue(node.isIncludeKeys());
        assertEquals(List.of("env", "dc"), node.getTagNames());
    }

    public void testFactoryMethodTagsOnly() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode tagNode1 = mock(ValueNode.class);
        ValueNode tagNode2 = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(tagNode1, tagNode2));
        when(tagNode1.getValue()).thenReturn("\"env\"");
        when(tagNode2.getValue()).thenReturn("\"dc\"");

        AliasByDistinctTagsPlanNode node = AliasByDistinctTagsPlanNode.of(functionNode);

        assertFalse(node.isIncludeKeys());
        assertEquals(List.of("env", "dc"), node.getTagNames());
    }

    public void testFactoryMethodNoArgs() {
        FunctionNode functionNode = mock(FunctionNode.class);
        when(functionNode.getChildren()).thenReturn(List.of());

        AliasByDistinctTagsPlanNode node = AliasByDistinctTagsPlanNode.of(functionNode);

        assertFalse(node.isIncludeKeys());
        assertNull(node.getTagNames());
    }

    public void testFactoryMethodInvalidArgType() {
        FunctionNode functionNode = mock(FunctionNode.class);
        FunctionNode invalidArg = mock(FunctionNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(invalidArg));

        assertThrows(IllegalArgumentException.class, () -> AliasByDistinctTagsPlanNode.of(functionNode));
    }

    public void testVisitorAccept() {
        AliasByDistinctTagsPlanNode node = new AliasByDistinctTagsPlanNode(1, true, List.of("env"));
        M3PlanVisitor<String> visitor = mock(M3PlanVisitor.class);
        when(visitor.visit(node)).thenReturn("visited");

        String result = node.accept(visitor);

        assertEquals("visited", result);
        verify(visitor).visit(node);
    }
}
