/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexMetricsDocValues;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexMetricsDocValues;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE;

/**
 * Unit tests for MetricsLeafReader implementations.
 */
public class MetricsLeafReaderTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter indexWriter;
    private MemChunkReader memChunkReader;
    private Map<Long, List<Chunk>> referenceToChunkMap;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        indexWriter = new IndexWriter(directory, config);

        // Initialize the reference to chunk mapping
        referenceToChunkMap = new HashMap<>();
        setupReferenceToChunkMapping();

        // Create a MemChunkReader that looks up chunks from the map
        memChunkReader = (reference) -> { return referenceToChunkMap.getOrDefault(reference, List.of()); };
    }

    @Override
    public void tearDown() throws Exception {
        if (indexWriter != null) {
            indexWriter.close();
        }
        if (directory != null) {
            directory.close();
        }

        super.tearDown();
    }

    /**
     * Sets up the reference to chunk mapping for testing.
     * This method populates the map with chunks corresponding to the references
     * used in createLiveSeriesTestDocuments().
     */
    private void setupReferenceToChunkMapping() {
        // Reference 100L: cpu_usage{host="server1", region="us-west"}
        XORChunk cpuChunk = new XORChunk();
        ChunkAppender cpuAppender = cpuChunk.appender();
        cpuAppender.append(1000L, 75.5);
        cpuAppender.append(2000L, 80.2);
        cpuAppender.append(3000L, 85.1);
        referenceToChunkMap.put(100L, List.of(cpuChunk));

        // Reference 200L: memory_usage{host="server2", region="us-east"}
        XORChunk memoryChunk = new XORChunk();
        ChunkAppender memoryAppender = memoryChunk.appender();
        memoryAppender.append(1000L, 2048.0);
        memoryAppender.append(2000L, 2560.0);
        memoryAppender.append(3000L, 3072.0);
        referenceToChunkMap.put(200L, List.of(memoryChunk));
    }

    /**
     * Test ClosedChunkIndexLeafReader functionality
     */
    public void testClosedChunkIndexLeafReader() throws IOException {
        // Create test documents with chunks and labels
        createClosedChunkTestDocuments();

        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create ClosedChunkIndexLeafReader
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader);

            // Test getMetricsDocValues()
            MetricsDocValues metricsDocValues = metricsReader.getMetricsDocValues();
            assertNotNull("MetricsDocValues should not be null", metricsDocValues);
            assertTrue("Should be ClosedChunkIndexMetricsDocValues", metricsDocValues instanceof ClosedChunkIndexMetricsDocValues);

            // Verify that chunk ref doc values throws UnsupportedOperationException
            expectThrows(UnsupportedOperationException.class, metricsDocValues::getChunkRefDocValues);

            // Test chunksForDoc() for document 0
            List<ChunkIterator> chunks = metricsReader.chunksForDoc(0, metricsDocValues);
            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk", 1, chunks.size());

            ChunkIterator chunk = chunks.get(0);
            assertNotNull("Chunk should not be null", chunk);
            assertTrue("Chunk should have data", chunk.next() != ChunkIterator.ValueType.NONE);

            // Test labelsForDoc() for document 0
            Labels labels = metricsReader.labelsForDoc(0, metricsDocValues);
            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have correct metric name", "http_requests_total", labels.get("__name__"));
            assertEquals("Should have correct method label", "GET", labels.get("method"));
            assertEquals("Should have correct status label", "200", labels.get("status"));
        }
    }

    /**
     * Test LiveSeriesIndexLeafReader functionality
     */
    public void testLiveSeriesIndexLeafReader() throws IOException {
        // Create test documents with references and labels
        createLiveSeriesTestDocuments();

        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create LiveSeriesIndexLeafReader
            LiveSeriesIndexLeafReader metricsReader = new LiveSeriesIndexLeafReader(leafReader, memChunkReader);

            // Test getMetricsDocValues()
            MetricsDocValues metricsDocValues = metricsReader.getMetricsDocValues();
            assertNotNull("MetricsDocValues should not be null", metricsDocValues);
            assertTrue("Should be LiveSeriesIndexMetricsDocValues", metricsDocValues instanceof LiveSeriesIndexMetricsDocValues);

            // Verify that chunk doc values throws UnsupportedOperationException
            expectThrows(UnsupportedOperationException.class, metricsDocValues::getChunkDocValues);

            // Test chunksForDoc() for document 0 (reference 100L)
            List<ChunkIterator> chunks = metricsReader.chunksForDoc(0, metricsDocValues);
            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk for reference 100L", 1, chunks.size());

            // Verify the chunk contains expected data
            ChunkIterator chunkIterator = chunks.get(0);
            assertTrue("Chunk should have data", chunkIterator.next() != ChunkIterator.ValueType.NONE);
            ChunkIterator.TimestampValue firstValue = chunkIterator.at();
            assertEquals("First timestamp should be 1000L", 1000L, firstValue.timestamp());
            assertEquals("First value should be 75.5", 75.5, firstValue.value(), 0.001);

            // Test labelsForDoc() for document 0
            Labels labels = metricsReader.labelsForDoc(0, metricsDocValues);
            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have correct metric name", "cpu_usage", labels.get("__name__"));
            assertEquals("Should have correct host label", "server1", labels.get("host"));
            assertEquals("Should have correct region label", "us-west", labels.get("region"));
        }
    }

    /**
     * Test error handling when chunk field is missing
     */
    public void testClosedChunkIndexLeafReaderMissingChunkField() throws IOException {
        // Create document without chunk field
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:test_metric")));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader);

            // Should throw IOException when chunk field is missing
            expectThrows(IOException.class, metricsReader::getMetricsDocValues);
        }
    }

    private void createClosedChunkTestDocuments() throws IOException {
        // Create MemChunk instances with XOR chunks and sample data
        MemChunk memChunk1 = new MemChunk(1, 1000L, 3000L, null);
        XORChunk chunk1 = new XORChunk();
        ChunkAppender appender1 = chunk1.appender();
        appender1.append(1000L, 42.0);
        appender1.append(2000L, 43.0);
        appender1.append(3000L, 44.0);
        memChunk1.setChunk(chunk1);

        MemChunk memChunk2 = new MemChunk(2, 4000L, 5000L, null);
        XORChunk chunk2 = new XORChunk();
        ChunkAppender appender2 = chunk2.appender();
        appender2.append(4000L, 100.0);
        appender2.append(5000L, 101.0);
        memChunk2.setChunk(chunk2);

        // Serialize chunks
        BytesRef serializedChunk1 = ClosedChunkIndexIO.serializeChunk(memChunk1.getChunk());
        BytesRef serializedChunk2 = ClosedChunkIndexIO.serializeChunk(memChunk2.getChunk());

        // Document 1: http_requests_total{method="GET", status="200"}
        Document doc1 = new Document();
        doc1.add(new BinaryDocValuesField(CHUNK, serializedChunk1));
        doc1.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:http_requests_total")));
        doc1.add(new SortedSetDocValuesField(LABELS, new BytesRef("method:GET")));
        doc1.add(new SortedSetDocValuesField(LABELS, new BytesRef("status:200")));
        indexWriter.addDocument(doc1);

        // Document 2: http_requests_total{method="POST", status="404"}
        Document doc2 = new Document();
        doc2.add(new BinaryDocValuesField(CHUNK, serializedChunk2));
        doc2.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:http_requests_total")));
        doc2.add(new SortedSetDocValuesField(LABELS, new BytesRef("method:POST")));
        doc2.add(new SortedSetDocValuesField(LABELS, new BytesRef("status:404")));
        indexWriter.addDocument(doc2);
    }

    private void createLiveSeriesTestDocuments() throws IOException {
        // Document 1: cpu_usage{host="server1", region="us-west"} with reference 100L
        Document doc1 = new Document();
        doc1.add(new NumericDocValuesField(REFERENCE, 100L));
        doc1.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:cpu_usage")));
        doc1.add(new SortedSetDocValuesField(LABELS, new BytesRef("host:server1")));
        doc1.add(new SortedSetDocValuesField(LABELS, new BytesRef("region:us-west")));
        indexWriter.addDocument(doc1);

        // Document 2: memory_usage{host="server2", region="us-east"} with reference 200L
        Document doc2 = new Document();
        doc2.add(new NumericDocValuesField(REFERENCE, 200L));
        doc2.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:memory_usage")));
        doc2.add(new SortedSetDocValuesField(LABELS, new BytesRef("host:server2")));
        doc2.add(new SortedSetDocValuesField(LABELS, new BytesRef("region:us-east")));
        indexWriter.addDocument(doc2);
    }
}
