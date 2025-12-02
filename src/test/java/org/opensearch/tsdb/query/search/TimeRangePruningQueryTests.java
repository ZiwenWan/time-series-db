/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TimeRangePruningQueryTests extends OpenSearchTestCase {

    public void testConstructor() {
        Query delegate = new MatchAllDocsQuery();
        long minTimestamp = 1000L;
        long maxTimestamp = 2000L;

        TimeRangePruningQuery query = new TimeRangePruningQuery(delegate, minTimestamp, maxTimestamp);

        assertEquals(delegate, query.getDelegate());
        assertEquals(minTimestamp, query.getMinTimestamp());
        assertEquals(maxTimestamp, query.getMaxTimestamp());
    }

    public void testConstructorNullDelegate() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new TimeRangePruningQuery(null, 1000L, 2000L));
        assertTrue(e.getMessage().contains("delegate query cannot be null"));
    }

    public void testToString() {
        Query delegate = new MatchAllDocsQuery();
        TimeRangePruningQuery query = new TimeRangePruningQuery(delegate, 1000L, 2000L);

        String result = query.toString("field");
        assertTrue(result.contains("TimeRangePruningQuery"));
        assertTrue(result.contains("delegate="));
        assertTrue(result.contains("minTimestamp=1000"));
        assertTrue(result.contains("maxTimestamp=2000"));
    }

    public void testEquals() {
        Query delegate1 = new MatchAllDocsQuery();
        Query delegate2 = new BooleanQuery.Builder().build();

        TimeRangePruningQuery query1 = new TimeRangePruningQuery(delegate1, 1000L, 2000L);
        TimeRangePruningQuery query2 = new TimeRangePruningQuery(delegate1, 1000L, 2000L);
        TimeRangePruningQuery query3 = new TimeRangePruningQuery(delegate2, 1000L, 2000L);
        TimeRangePruningQuery query4 = new TimeRangePruningQuery(delegate1, 1500L, 2000L);
        TimeRangePruningQuery query5 = new TimeRangePruningQuery(delegate1, 1000L, 2500L);

        assertEquals(query1, query2);
        assertNotEquals(query1, query3);
        assertNotEquals(query1, query4);
        assertNotEquals(query1, query5);
        assertNotEquals(null, query1);
        assertNotEquals(new MatchAllDocsQuery(), query1);
    }

    public void testHashCode() {
        Query delegate = new MatchAllDocsQuery();
        TimeRangePruningQuery query1 = new TimeRangePruningQuery(delegate, 1000L, 2000L);
        TimeRangePruningQuery query2 = new TimeRangePruningQuery(delegate, 1000L, 2000L);

        assertEquals(query1.hashCode(), query2.hashCode());
    }

    public void testRewriteNoRewritingNeeded() throws IOException {
        // Use MatchAllDocsQuery which doesn't need rewriting
        Query delegate = new MatchAllDocsQuery();
        TimeRangePruningQuery query = new TimeRangePruningQuery(delegate, 1000L, 2000L);

        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        writer.addDocument(new Document());
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Verify delegate doesn't rewrite
        assertEquals(delegate, delegate.rewrite(searcher));

        // Rewrite should return the same instance (no rewriting needed)
        Query rewritten = query.rewrite(searcher);
        assertSame(query, rewritten);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testRewriteWithDelegateRewrite() throws IOException {
        // Use BooleanQuery which may rewrite
        Query delegate = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST).build();
        TimeRangePruningQuery query = new TimeRangePruningQuery(delegate, 1000L, 2000L);

        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        writer.addDocument(new Document());
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query rewritten = query.rewrite(searcher);

        // If delegate rewrites, we should get a new TimeRangePruningQuery
        Query rewrittenDelegate = delegate.rewrite(searcher);
        if (rewrittenDelegate != delegate) {
            assertTrue(rewritten instanceof TimeRangePruningQuery);
            assertNotSame(query, rewritten);
            TimeRangePruningQuery rewrittenPruning = (TimeRangePruningQuery) rewritten;
            assertEquals(rewrittenDelegate, rewrittenPruning.getDelegate());
            assertEquals(query.getMinTimestamp(), rewrittenPruning.getMinTimestamp());
            assertEquals(query.getMaxTimestamp(), rewrittenPruning.getMaxTimestamp());
        }

        reader.close();
        writer.close();
        dir.close();
    }

    public void testCreateWeight() throws IOException {
        Query delegate = new MatchAllDocsQuery();
        TimeRangePruningQuery query = new TimeRangePruningQuery(delegate, 1000L, 2000L);

        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        writer.addDocument(new Document());
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);

        assertNotNull(weight);
        assertTrue(weight instanceof TimeRangePruningWeight);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testVisit() {
        // Create a query with a term that we can track
        Term term = new Term("field", "value");
        Query delegate = new TermQuery(term);
        TimeRangePruningQuery query = new TimeRangePruningQuery(delegate, 1000L, 2000L);

        // Track if visitLeaf was called and which fields were visited
        List<String> visitedFields = new ArrayList<>();
        QueryVisitor visitor = new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                visitedFields.add(field);
                return true;
            }
        };

        // Visit the query
        query.visit(visitor);

        // Verify that the delegate was visited (field should be tracked)
        assertEquals(1, visitedFields.size());
        assertEquals("field", visitedFields.getFirst());
    }
}
