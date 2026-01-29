/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests for UnionFunctionTransformation to ensure union function syntax
 * produces the same AST as equivalent pipe syntax.
 */
public class UnionFunctionTransformationTests extends OpenSearchTestCase {

    /**
     * Test that union function syntax produces the same flattened AST as the equivalent pipeline.
     * The union function should create a flat pipeline just like the pipe operator with multiple fetches.
     */
    public void testUnionSyntaxEquivalence() throws Exception {
        // Test case: union function should be equivalent to multiple fetch pipeline
        String expr1 = "fetch country:us";
        String expr2 = "fetch state:xyz";
        String expr3 = "fetch city:toronto";

        String unionSyntax = String.format("union (%s) (%s) (%s)", expr1, expr2, expr3);
        String equivalentPipeSyntax = String.format("%s | %s | %s", expr1, expr2, expr3);

        assertASTEqual(unionSyntax, equivalentPipeSyntax, "union() should equal flat pipe syntax");
    }

    /**
     * Test union with complex expressions that contain pipe operations within each argument.
     */
    public void testUnionWithComplexExpressions() throws Exception {
        String expr1 = "fetch name:errors | transformNull 0";
        String expr2 = "fetch name:requests | sum region";

        String unionSyntax = String.format("union (%s) (%s)", expr1, expr2);
        String equivalentPipeSyntax = String.format("%s | %s", expr1, expr2);

        assertASTEqual(unionSyntax, equivalentPipeSyntax, "union() with complex expressions");
    }

    /**
     * Test simple two-expression union equivalence.
     */
    public void testSimpleUnionEquivalence() throws Exception {
        String expr1 = "fetch name:metric1";
        String expr2 = "fetch name:metric2";

        String unionSyntax = String.format("union (%s) (%s)", expr1, expr2);
        String pipeSyntax = String.format("%s | %s", expr1, expr2);

        assertASTEqual(unionSyntax, pipeSyntax, "Simple two-expression union");
    }

    /**
     * Test complex pipeline expressions in union.
     */
    public void testComplexPipelineUnion() throws Exception {
        String expr1 = "fetch name:errors | transformNull 0 | sum";
        String expr2 = "fetch name:requests | avg region";

        String unionSyntax = String.format("union (%s) (%s)", expr1, expr2);
        String pipeSyntax = String.format("%s | %s", expr1, expr2);

        assertASTEqual(unionSyntax, pipeSyntax, "Complex pipeline union");
    }

    /**
     * Test the transformation directly on AST nodes.
     */
    public void testUnionFunctionTransformationDirect() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        // Create union function node: union (fetch name:test1) (fetch name:test2)
        FunctionNode unionFunction = new FunctionNode();
        unionFunction.setFunctionName("union");

        // Create first argument: (fetch name:test1)
        GroupNode group1 = new GroupNode();
        PipelineNode pipeline1 = new PipelineNode();
        FunctionNode fetch1 = createFetchFunction("test1");
        pipeline1.addChildNode(fetch1);
        group1.addChildNode(pipeline1);
        unionFunction.addChildNode(group1);

        // Create second argument: (fetch name:test2)
        GroupNode group2 = new GroupNode();
        PipelineNode pipeline2 = new PipelineNode();
        FunctionNode fetch2 = createFetchFunction("test2");
        pipeline2.addChildNode(fetch2);
        group2.addChildNode(pipeline2);
        unionFunction.addChildNode(group2);

        // Test transformation
        assertTrue("Should be able to transform union function", transformation.canTransform(unionFunction));

        List<M3ASTNode> result = transformation.transform(unionFunction);
        assertEquals("Should return 2 function nodes", 2, result.size());

        // Verify the results are the fetch functions
        assertTrue("First result should be FunctionNode", result.get(0) instanceof FunctionNode);
        assertTrue("Second result should be FunctionNode", result.get(1) instanceof FunctionNode);

        FunctionNode firstFetch = (FunctionNode) result.get(0);
        FunctionNode secondFetch = (FunctionNode) result.get(1);

        assertEquals("First fetch should be 'fetch'", "fetch", firstFetch.getFunctionName());
        assertEquals("Second fetch should be 'fetch'", "fetch", secondFetch.getFunctionName());
    }

    /**
     * Test that non-union functions are not transformed.
     */
    public void testNonUnionFunctionNotTransformed() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        FunctionNode sumFunction = createFunctionNode("sum");
        assertFalse("Should not transform non-union functions", transformation.canTransform(sumFunction));

        FunctionNode fetchFunction = createFunctionNode("fetch");
        assertFalse("Should not transform fetch functions", transformation.canTransform(fetchFunction));
    }

    /**
     * Test validation of minimum arguments.
     */
    public void testUnionFunctionValidation() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        // Test with only 1 argument (should fail)
        FunctionNode unionFunction = new FunctionNode();
        unionFunction.setFunctionName("union");
        GroupNode singleGroup = new GroupNode();
        unionFunction.addChildNode(singleGroup);

        assertTrue("Should be able to transform union function", transformation.canTransform(unionFunction));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { transformation.transform(unionFunction); }
        );

        assertTrue(
            "Error message should mention minimum arguments",
            exception.getMessage().contains("union function requires at least 2 arguments")
        );
    }

    /**
     * Helper method to compare ASTs from two different query syntaxes.
     */
    private void assertASTEqual(String query1, String query2, String testCase) throws Exception {
        // Parse both queries with AST processing
        M3ASTNode ast1 = M3QLParser.parse(query1, true);
        M3ASTNode ast2 = M3QLParser.parse(query2, true);

        // Convert both ASTs to string representation for comparison
        String ast1String = astToString(ast1);
        String ast2String = astToString(ast2);

        assertEquals(testCase + ": ASTs should be identical", ast2String, ast1String);
    }

    /**
     * Convert AST to string representation for comparison.
     */
    private String astToString(M3ASTNode node) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
            printAST(node, 0, ps);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            fail("Failed to convert AST to string: " + e.getMessage());
            return null;
        }
    }

    /**
     * Print AST structure (similar to M3TestUtils.printAST).
     */
    private void printAST(M3ASTNode node, int depth, PrintStream out) {
        // Print indentation
        for (int i = 0; i < depth; i++) {
            out.print("  ");
        }

        // Print node type and name
        out.println(node.getExplainName());

        // Print children recursively
        for (M3ASTNode child : node.getChildren()) {
            printAST(child, depth + 1, out);
        }
    }

    /**
     * Helper to create a basic function node.
     */
    private FunctionNode createFunctionNode(String name) {
        FunctionNode function = new FunctionNode();
        function.setFunctionName(name);
        return function;
    }

    /**
     * Helper to create a fetch function with a name tag.
     */
    private FunctionNode createFetchFunction(String nameValue) {
        // For simplicity, just create a basic fetch function
        // In real scenarios, this would include tag parsing
        FunctionNode fetch = new FunctionNode();
        fetch.setFunctionName("fetch");
        return fetch;
    }
}
