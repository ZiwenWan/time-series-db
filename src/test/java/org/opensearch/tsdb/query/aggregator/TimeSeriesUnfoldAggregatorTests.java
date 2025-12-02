/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TimeSeriesUnfoldAggregator.
 * Focuses on testing leaf pruning optimization and collector behavior.
 */
public class TimeSeriesUnfoldAggregatorTests extends OpenSearchTestCase {
    //
    // /**
    // * Tests that when the leaf reader is not a TSDBLeafReader (null after unwrapping),
    // * the aggregator returns the sub-collector without processing, effectively pruning the segment.
    // */
    // public void testGetLeafCollectorWithNonTSDBLeafReader() throws IOException {
    // long minTimestamp = 1000L;
    // long maxTimestamp = 5000L;
    // long step = 100L;
    //
    // TimeSeriesUnfoldAggregator aggregator = createAggregator(minTimestamp, maxTimestamp, step);
    //
    // // Create a regular Lucene LeafReader (not a TSDBLeafReader)
    // Directory directory = new ByteBuffersDirectory();
    // IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
    // writer.addDocument(new Document());
    // writer.commit();
    // DirectoryReader reader = DirectoryReader.open(writer);
    // writer.close();
    //
    // LeafReaderContext ctx = reader.leaves().get(0);
    // LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);
    //
    // // Act - Get leaf collector for non-TSDB reader
    // LeafBucketCollector result = aggregator.getLeafCollector(ctx, mockSubCollector);
    //
    // // Assert - Should return the sub-collector directly (pruning happened)
    // assertSame("Should return sub-collector when reader is not TSDBLeafReader", mockSubCollector, result);
    //
    // // Cleanup
    // reader.close();
    // directory.close();
    // aggregator.close();
    // }

    /**
     * Tests that when the TSDBLeafReader does not overlap with the query time range,
     * the aggregator returns the sub-collector without processing, effectively pruning the segment.
     */
    public void testGetLeafCollectorWithNonOverlappingTimeRange() throws IOException {
        long queryMinTimestamp = 1000L;
        long queryMaxTimestamp = 5000L;
        long step = 100L;

        TimeSeriesUnfoldAggregator aggregator = createAggregator(queryMinTimestamp, queryMaxTimestamp, step);

        long leafMinTimestamp = 6000L;
        long leafMaxTimestamp = 10000L;

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(leafMinTimestamp, leafMaxTimestamp);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSubCollector);
        assertSame("Should return sub-collector when leaf does not overlap time range", mockSubCollector, result);
        assertFalse("Leaf should not overlap with query range", readerCtx.reader.overlapsTimeRange(queryMinTimestamp, queryMaxTimestamp));

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    /**
     * Tests that when the TSDBLeafReader overlaps with the query time range,
     * the aggregator returns a TimeSeriesUnfoldLeafBucketCollector (not the sub-collector).
     */
    public void testGetLeafCollectorWithOverlappingTimeRange() throws IOException {
        long queryMinTimestamp = 1000L;
        long queryMaxTimestamp = 5000L;
        long step = 100L;

        TimeSeriesUnfoldAggregator aggregator = createAggregator(queryMinTimestamp, queryMaxTimestamp, step);

        long leafMinTimestamp = 2000L;
        long leafMaxTimestamp = 6000L;

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(leafMinTimestamp, leafMaxTimestamp);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSubCollector);

        assertNotSame("Should return new collector when leaf overlaps time range", mockSubCollector, result);
        assertNotNull("Should return a non-null collector", result);
        assertTrue("Leaf should overlap with query range", readerCtx.reader.overlapsTimeRange(queryMinTimestamp, queryMaxTimestamp));

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    /**
     * Tests edge case where leaf time range ends exactly at query start (no overlap).
     */
    public void testGetLeafCollectorWithLeafEndingAtQueryStart() throws IOException {
        // Arrange - Create aggregator with query time range [5000, 10000)
        long queryMinTimestamp = 5000L;
        long queryMaxTimestamp = 10000L;
        long step = 100L;

        TimeSeriesUnfoldAggregator aggregator = createAggregator(queryMinTimestamp, queryMaxTimestamp, step);

        long leafMinTimestamp = 1000L;
        long leafMaxTimestamp = 4999L;

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(leafMinTimestamp, leafMaxTimestamp);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSubCollector);

        assertSame("Should return sub-collector when leaf ends before query start", mockSubCollector, result);
        assertFalse("Leaf should not overlap with query range", readerCtx.reader.overlapsTimeRange(queryMinTimestamp, queryMaxTimestamp));

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    /**
     * Tests edge case where leaf time range starts exactly at query end (no overlap).
     */
    public void testGetLeafCollectorWithLeafStartingAtQueryEnd() throws IOException {
        long queryMinTimestamp = 1000L;
        long queryMaxTimestamp = 5000L;
        long step = 100L;

        TimeSeriesUnfoldAggregator aggregator = createAggregator(queryMinTimestamp, queryMaxTimestamp, step);

        long leafMinTimestamp = 5000L;
        long leafMaxTimestamp = 10000L;

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(leafMinTimestamp, leafMaxTimestamp);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSubCollector);

        assertSame("Should return sub-collector when leaf starts at exclusive query end", mockSubCollector, result);
        assertFalse("Leaf should not overlap with query range", readerCtx.reader.overlapsTimeRange(queryMinTimestamp, queryMaxTimestamp));

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    /**
     * Tests that leaf with partial overlap is not pruned.
     */
    public void testGetLeafCollectorWithPartialOverlap() throws IOException {
        long queryMinTimestamp = 3000L;
        long queryMaxTimestamp = 7000L;
        long step = 100L;

        TimeSeriesUnfoldAggregator aggregator = createAggregator(queryMinTimestamp, queryMaxTimestamp, step);

        long leafMinTimestamp = 1000L;
        long leafMaxTimestamp = 5000L;

        TSDBLeafReaderWithContext readerCtx = createMockTSDBLeafReaderWithContext(leafMinTimestamp, leafMaxTimestamp);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        LeafBucketCollector result = aggregator.getLeafCollector(readerCtx.context, mockSubCollector);

        assertNotSame("Should return new collector when leaf partially overlaps", mockSubCollector, result);
        assertTrue("Leaf should overlap with query range", readerCtx.reader.overlapsTimeRange(queryMinTimestamp, queryMaxTimestamp));

        readerCtx.directoryReader.close();
        readerCtx.directory.close();
        aggregator.close();
    }

    /**
     * Creates a TimeSeriesUnfoldAggregator for testing.
     */
    private TimeSeriesUnfoldAggregator createAggregator(long minTimestamp, long maxTimestamp, long step) throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);

        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");

        when(mockSearchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        when(mockSearchContext.bigArrays()).thenReturn(bigArrays);

        return new TimeSeriesUnfoldAggregator(
            "test_aggregator",
            AggregatorFactories.EMPTY,
            List.of(),  // No pipeline stages for these tests
            mockSearchContext,
            null,  // No parent
            CardinalityUpperBound.NONE,
            minTimestamp,
            maxTimestamp,
            step,
            Map.of()
        );
    }

    /**
     * Creates a mock TSDBLeafReader with specified time bounds and returns both the reader and its context.
     * Uses a concrete implementation to allow the overlapsTimeRange method to work properly.
     */
    private static class TSDBLeafReaderWithContext {
        final TSDBLeafReader reader;
        final LeafReaderContext context;
        final DirectoryReader directoryReader;
        final Directory directory;
        final IndexWriter indexWriter;

        TSDBLeafReaderWithContext(
            TSDBLeafReader reader,
            LeafReaderContext context,
            DirectoryReader directoryReader,
            Directory directory,
            IndexWriter indexWriter
        ) {
            this.reader = reader;
            this.context = context;
            this.directoryReader = directoryReader;
            this.directory = directory;
            this.indexWriter = indexWriter;
        }
    }

    private TSDBLeafReaderWithContext createMockTSDBLeafReaderWithContext(long minTimestamp, long maxTimestamp) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());
        indexWriter.addDocument(new Document());
        indexWriter.commit();

        // Open a DirectoryReader to get a real leaf reader
        DirectoryReader tempReader = DirectoryReader.open(indexWriter);
        LeafReader baseReader = tempReader.leaves().get(0).reader();

        // Create a TSDBLeafReader wrapping the base reader
        TSDBLeafReader tsdbLeafReader = new TSDBLeafReader(baseReader, minTimestamp, maxTimestamp) {
            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return reader;
            }

            @Override
            public TSDBDocValues getTSDBDocValues() throws IOException {
                return mock(TSDBDocValues.class);
            }

            @Override
            public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return List.of();
            }

            @Override
            public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return mock(Labels.class);
            }
        };

        // Create a CompositeReader that wraps our TSDBLeafReader, so we can get a proper LeafReaderContext
        CompositeReader compositeReader = new CompositeReader() {
            @Override
            protected List<? extends LeafReader> getSequentialSubReaders() {
                return Collections.singletonList(tsdbLeafReader);
            }

            @Override
            public TermVectors termVectors() throws IOException {
                return tsdbLeafReader.termVectors();
            }

            @Override
            public int numDocs() {
                return tsdbLeafReader.numDocs();
            }

            @Override
            public int maxDoc() {
                return tsdbLeafReader.maxDoc();
            }

            @Override
            public StoredFields storedFields() throws IOException {
                return tsdbLeafReader.storedFields();
            }

            @Override
            protected void doClose() throws IOException {
                // No-op, we'll close the readers manually
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public int docFreq(Term term) throws IOException {
                return tsdbLeafReader.docFreq(term);
            }

            @Override
            public long totalTermFreq(Term term) throws IOException {
                return tsdbLeafReader.totalTermFreq(term);
            }

            @Override
            public long getSumDocFreq(String field) throws IOException {
                return tsdbLeafReader.getSumDocFreq(field);
            }

            @Override
            public int getDocCount(String field) throws IOException {
                return tsdbLeafReader.getDocCount(field);
            }

            @Override
            public long getSumTotalTermFreq(String field) throws IOException {
                return tsdbLeafReader.getSumTotalTermFreq(field);
            }
        };

        // Get the context from the composite reader
        LeafReaderContext context = compositeReader.leaves().getFirst();

        return new TSDBLeafReaderWithContext(tsdbLeafReader, context, tempReader, directory, indexWriter);
    }
}
