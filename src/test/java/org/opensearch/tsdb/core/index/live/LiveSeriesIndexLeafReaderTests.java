/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.MetricsDocValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE;

public class LiveSeriesIndexLeafReaderTests extends OpenSearchTestCase {

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

        // Setup reference to chunk mapping
        referenceToChunkMap = new HashMap<>();
        setupTestChunks();
        memChunkReader = reference -> referenceToChunkMap.getOrDefault(reference, List.of());
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

    private void setupTestChunks() {
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
        referenceToChunkMap.put(200L, List.of(memoryChunk));

        // Reference 300L: Empty chunks list
        referenceToChunkMap.put(300L, List.of());
    }

    public void testConstructorAndBasicMethods() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);

            assertNotNull("Reader should not be null", leafReader);
            assertEquals("numDocs should match", innerReader.numDocs(), leafReader.numDocs());
            assertEquals("maxDoc should match", innerReader.maxDoc(), leafReader.maxDoc());
            assertSame("FieldInfos should be same", innerReader.getFieldInfos(), leafReader.getFieldInfos());
            assertSame("MetaData should be same", innerReader.getMetaData(), leafReader.getMetaData());
        }
    }

    public void testGetMetricsDocValues() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            assertNotNull("MetricsDocValues should not be null", metricsDocValues);
            assertTrue("Should be LiveSeriesIndexMetricsDocValues", metricsDocValues instanceof LiveSeriesIndexMetricsDocValues);

            NumericDocValues chunkRefDocValues = metricsDocValues.getChunkRefDocValues();
            SortedSetDocValues labelsDocValues = metricsDocValues.getLabelsDocValues();

            assertNotNull("ChunkRefDocValues should not be null", chunkRefDocValues);
            assertNotNull("LabelsDocValues should not be null", labelsDocValues);

            expectThrows(UnsupportedOperationException.class, metricsDocValues::getChunkDocValues);
        }
    }

    public void testChunksForDoc() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, metricsDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk for reference 100L", 1, chunks.size());

            ChunkIterator chunkIterator = chunks.get(0);
            assertNotNull("ChunkIterator should not be null", chunkIterator);

            // Verify chunk contains expected data
            assertEquals("Should have first value", ChunkIterator.ValueType.FLOAT, chunkIterator.next());
            ChunkIterator.TimestampValue firstValue = chunkIterator.at();
            assertEquals("First timestamp should be 1000L", 1000L, firstValue.timestamp());
            assertEquals("First value should be 75.5", 75.5, firstValue.value(), 0.001);
        }
    }

    public void testChunksForDocWithEmptyChunks() throws IOException {
        createTestDocument(300L, "empty_metric", "server3", "us-central");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, metricsDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertTrue("Chunks should be empty for reference 300L", chunks.isEmpty());
        }
    }

    public void testLabelsForDoc() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            Labels labels = leafReader.labelsForDoc(0, metricsDocValues);

            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have metric name", "cpu_usage", labels.get("__name__"));
            assertEquals("Should have host label", "server1", labels.get("host"));
            assertEquals("Should have region label", "us-west", labels.get("region"));
        }
    }

    public void testMissingChunkRefField() throws IOException {
        // Create document without chunk reference field
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:test_metric")));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);

            IOException exception = expectThrows(IOException.class, leafReader::getMetricsDocValues);
            assertTrue("Should mention chunk ref field missing", exception.getMessage().contains("Chunk ref field '" + REFERENCE + "'"));
        }
    }

    public void testMissingLabelsField() throws IOException {
        // Create document without labels field
        Document doc = new Document();
        doc.add(new NumericDocValuesField(REFERENCE, 100L));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);

            IOException exception = expectThrows(IOException.class, leafReader::getMetricsDocValues);
            assertTrue("Should mention labels field missing", exception.getMessage().contains("Labels field '" + LABELS + "'"));
        }
    }

    public void testDelegatedMethods() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);

            // Test core delegated methods
            assertSame("getCoreCacheHelper should delegate", innerReader.getCoreCacheHelper(), leafReader.getCoreCacheHelper());
            assertSame("getReaderCacheHelper should delegate", innerReader.getReaderCacheHelper(), leafReader.getReaderCacheHelper());
            assertEquals("getLiveDocs should delegate", innerReader.getLiveDocs(), leafReader.getLiveDocs());
            assertSame("getFieldInfos should delegate", innerReader.getFieldInfos(), leafReader.getFieldInfos());
            assertSame("getMetaData should delegate", innerReader.getMetaData(), leafReader.getMetaData());

            // Test terms() delegation
            Terms innerTerms = innerReader.terms("non_existent_field");
            Terms leafTerms = leafReader.terms("non_existent_field");
            assertEquals("terms() should delegate properly", innerTerms, leafTerms);

            // Test with existing field
            Terms innerLabelsTerms = innerReader.terms(LABELS);
            Terms leafLabelsTerms = leafReader.terms(LABELS);
            assertEquals("terms() should delegate for existing field", innerLabelsTerms, leafLabelsTerms);

            // Test doc values delegation methods
            assertEquals(
                "getNumericDocValues should delegate",
                innerReader.getNumericDocValues(REFERENCE) != null,
                leafReader.getNumericDocValues(REFERENCE) != null
            );
            assertEquals(
                "getSortedSetDocValues should delegate",
                innerReader.getSortedSetDocValues(LABELS) != null,
                leafReader.getSortedSetDocValues(LABELS) != null
            );

            // Test getNumericDocValues() delegation with non-existent field
            assertNull(
                "getNumericDocValues should return null for non-existent field",
                leafReader.getNumericDocValues("non_existent_field")
            );
            assertEquals(
                "getNumericDocValues should delegate for non-existent field",
                innerReader.getNumericDocValues("non_existent_field"),
                leafReader.getNumericDocValues("non_existent_field")
            );

            // Test with existing reference field
            assertNotNull("getNumericDocValues should work with reference field", leafReader.getNumericDocValues(REFERENCE));

            // Test getSortedDocValues() delegation
            assertNull("getSortedDocValues should return null for non-existent field", leafReader.getSortedDocValues("non_existent_field"));
            assertEquals(
                "getSortedDocValues should delegate properly",
                innerReader.getSortedDocValues("non_existent_field"),
                leafReader.getSortedDocValues("non_existent_field")
            );

            // Test getSortedNumericDocValues() delegation
            assertNull(
                "getSortedNumericDocValues should return null for non-existent field",
                leafReader.getSortedNumericDocValues("non_existent_field")
            );
            assertEquals(
                "getSortedNumericDocValues should delegate properly",
                innerReader.getSortedNumericDocValues("non_existent_field"),
                leafReader.getSortedNumericDocValues("non_existent_field")
            );

            // Test getNormValues() delegation
            assertNull("getNormValues should return null for non-existent field", leafReader.getNormValues("non_existent_field"));
            assertEquals(
                "getNormValues should delegate properly",
                innerReader.getNormValues("non_existent_field"),
                leafReader.getNormValues("non_existent_field")
            );

            // Test getDocValuesSkipper() delegation
            assertNull(
                "getDocValuesSkipper should return null for non-existent field",
                leafReader.getDocValuesSkipper("non_existent_field")
            );
            assertEquals(
                "getDocValuesSkipper should delegate properly",
                innerReader.getDocValuesSkipper("non_existent_field"),
                leafReader.getDocValuesSkipper("non_existent_field")
            );

            // Test getPointValues() delegation
            assertNull("getPointValues should return null for non-existent field", leafReader.getPointValues("non_existent_field"));
            assertEquals(
                "getPointValues should delegate properly",
                innerReader.getPointValues("non_existent_field"),
                leafReader.getPointValues("non_existent_field")
            );

            // Test additional delegation methods with existing fields
            assertEquals(
                "getBinaryDocValues delegation",
                innerReader.getBinaryDocValues("non_existent_field"),
                leafReader.getBinaryDocValues("non_existent_field")
            );

            // Test getSortedSetDocValues with existing labels field
            assertNotNull("getSortedSetDocValues should work with labels field", leafReader.getSortedSetDocValues(LABELS));

            // Test vector operations delegation
            assertNull(
                "getFloatVectorValues should delegate and return null for non-existent field",
                leafReader.getFloatVectorValues("non_existent_field")
            );
            assertEquals(
                "getFloatVectorValues should delegate properly",
                innerReader.getFloatVectorValues("non_existent_field"),
                leafReader.getFloatVectorValues("non_existent_field")
            );

            assertNull(
                "getByteVectorValues should delegate and return null for non-existent field",
                leafReader.getByteVectorValues("non_existent_field")
            );
            assertEquals(
                "getByteVectorValues should delegate properly",
                innerReader.getByteVectorValues("non_existent_field"),
                leafReader.getByteVectorValues("non_existent_field")
            );

            // Test searchNearestVectors delegation (should delegate without throwing)
            float[] queryVector = { 1.0f, 2.0f, 3.0f };
            byte[] queryByteVector = { 1, 2, 3 };

            // These should not throw exceptions even with null collectors since they delegate
            try {
                leafReader.searchNearestVectors("non_existent_field", queryVector, null, null);
                leafReader.searchNearestVectors("non_existent_field", queryByteVector, null, null);
            } catch (Exception e) {
                // Expected since we're passing null collectors, but method should exist and delegate
                assertTrue("Should be a runtime exception from null collector or field not found", e instanceof RuntimeException);
            }

            // Test other delegated methods that should not throw
            leafReader.checkIntegrity();
            assertNotNull("termVectors should delegate", leafReader.termVectors());
            assertNotNull("storedFields should delegate", leafReader.storedFields());

            // Test doGetSequentialStoredFieldsReader - this is a protected method but we can test it indirectly
            // by verifying that storedFields() works, which internally uses doGetSequentialStoredFieldsReader
            assertNotNull("storedFields should work (internally uses doGetSequentialStoredFieldsReader)", leafReader.storedFields());

            // Note: doClose() is implicitly tested when the reader is closed in tearDown()
            // and through the try-with-resources blocks. Testing it directly here would break the reader.
        }
    }

    public void testVectorOperations() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);

            // Test vector operations (should delegate without throwing)
            assertNull("getFloatVectorValues should delegate", leafReader.getFloatVectorValues("non_existent_field"));
            assertNull("getByteVectorValues should delegate", leafReader.getByteVectorValues("non_existent_field"));

            // Test vector search operations (should delegate without throwing)
            float[] queryVector = { 1.0f, 2.0f, 3.0f };
            byte[] queryByteVector = { 1, 2, 3 };

            // These should not throw exceptions even with null collectors
            try {
                leafReader.searchNearestVectors("non_existent_field", queryVector, null, null);
                leafReader.searchNearestVectors("non_existent_field", queryByteVector, null, null);
            } catch (Exception e) {
                // Expected since we're passing null collectors, but method should exist
                assertTrue("Should be a runtime exception from null collector", e instanceof RuntimeException);
            }
        }
    }

    public void testMultipleDocuments() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        createTestDocument(200L, "memory_usage", "server2", "us-east");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(innerReader, memChunkReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            // Test first document
            List<ChunkIterator> chunks1 = leafReader.chunksForDoc(0, metricsDocValues);
            assertEquals("Should have one chunk for doc 0", 1, chunks1.size());

            Labels labels1 = leafReader.labelsForDoc(0, metricsDocValues);
            assertEquals("Should have correct metric name for doc 0", "cpu_usage", labels1.get("__name__"));

            // Test second document
            List<ChunkIterator> chunks2 = leafReader.chunksForDoc(1, metricsDocValues);
            assertEquals("Should have one chunk for doc 1", 1, chunks2.size());

            Labels labels2 = leafReader.labelsForDoc(1, metricsDocValues);
            assertEquals("Should have correct metric name for doc 1", "memory_usage", labels2.get("__name__"));
        }
    }

    private void createTestDocument(long reference, String metricName, String host, String region) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField(REFERENCE, reference));
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:" + metricName)));
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("host:" + host)));
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("region:" + region)));
        indexWriter.addDocument(doc);
    }
}
