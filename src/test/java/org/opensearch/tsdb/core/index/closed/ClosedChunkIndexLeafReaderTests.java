/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.MetricsDocValues;

import java.io.IOException;
import java.util.List;

import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK;

public class ClosedChunkIndexLeafReaderTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter indexWriter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        indexWriter = new IndexWriter(directory, config);
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

    public void testConstructorAndBasicMethods() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);

            assertNotNull("Reader should not be null", leafReader);
            assertEquals("numDocs should match", innerReader.numDocs(), leafReader.numDocs());
            assertEquals("maxDoc should match", innerReader.maxDoc(), leafReader.maxDoc());
            assertSame("FieldInfos should be same", innerReader.getFieldInfos(), leafReader.getFieldInfos());
            assertSame("MetaData should be same", innerReader.getMetaData(), leafReader.getMetaData());
        }
    }

    public void testGetMetricsDocValues() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            assertNotNull("MetricsDocValues should not be null", metricsDocValues);
            assertTrue("Should be ClosedChunkIndexMetricsDocValues", metricsDocValues instanceof ClosedChunkIndexMetricsDocValues);

            BinaryDocValues chunkDocValues = metricsDocValues.getChunkDocValues();
            SortedSetDocValues labelsDocValues = metricsDocValues.getLabelsDocValues();

            assertNotNull("ChunkDocValues should not be null", chunkDocValues);
            assertNotNull("LabelsDocValues should not be null", labelsDocValues);

            expectThrows(UnsupportedOperationException.class, metricsDocValues::getChunkRefDocValues);
        }
    }

    public void testChunksForDoc() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, metricsDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk", 1, chunks.size());

            ChunkIterator chunkIterator = chunks.get(0);
            assertNotNull("ChunkIterator should not be null", chunkIterator);

            // Verify chunk contains expected data
            assertEquals("Should have first value", ChunkIterator.ValueType.FLOAT, chunkIterator.next());
            ChunkIterator.TimestampValue firstValue = chunkIterator.at();
            assertEquals("First timestamp should be 1000", 1000L, firstValue.timestamp());
            assertEquals("First value should be 42.0", 42.0, firstValue.value(), 0.001);
        }
    }

    public void testLabelsForDoc() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            Labels labels = leafReader.labelsForDoc(0, metricsDocValues);

            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have metric name", "test_metric", labels.get("__name__"));
            assertEquals("Should have host label", "server1", labels.get("host"));
        }
    }

    public void testChunksForDocWithEmptyChunk() throws IOException {
        // Create document with empty chunk data
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, new BytesRef(new byte[0])));
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:empty_metric")));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);
            MetricsDocValues metricsDocValues = leafReader.getMetricsDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, metricsDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertTrue("Chunks should be empty for empty chunk data", chunks.isEmpty());
        }
    }

    public void testMissingChunkField() throws IOException {
        // Create document without chunk field
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:test_metric")));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);

            IOException exception = expectThrows(IOException.class, leafReader::getMetricsDocValues);
            assertTrue("Should mention chunk field missing", exception.getMessage().contains("Chunk field '" + CHUNK + "'"));
        }
    }

    public void testMissingLabelsField() throws IOException {
        // Create test chunk
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        appender.append(1000L, 42.0);
        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(chunk);

        // Create document without labels field
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);

            IOException exception = expectThrows(IOException.class, leafReader::getMetricsDocValues);
            assertTrue("Should mention labels field missing", exception.getMessage().contains("Labels field '" + LABELS + "'"));
        }
    }

    public void testDelegatedMethods() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader);

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
                "getBinaryDocValues should delegate",
                innerReader.getBinaryDocValues(CHUNK) != null,
                leafReader.getBinaryDocValues(CHUNK) != null
            );
            assertEquals(
                "getSortedSetDocValues should delegate",
                innerReader.getSortedSetDocValues(LABELS) != null,
                leafReader.getSortedSetDocValues(LABELS) != null
            );

            // Test getNumericDocValues() delegation
            assertNull(
                "getNumericDocValues should return null for non-existent field",
                leafReader.getNumericDocValues("non_existent_field")
            );
            assertEquals(
                "getNumericDocValues should delegate for non-existent field",
                innerReader.getNumericDocValues("non_existent_field"),
                leafReader.getNumericDocValues("non_existent_field")
            );

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
                "getBinaryDocValues delegation for non-existent field",
                innerReader.getBinaryDocValues("non_existent_field"),
                leafReader.getBinaryDocValues("non_existent_field")
            );

            // Test with existing chunk field
            assertNotNull("getBinaryDocValues should work with chunk field", leafReader.getBinaryDocValues(CHUNK));

            // Test with existing labels field
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

    private void createTestDocument() throws IOException {
        // Create test chunk
        MemChunk memChunk = new MemChunk(1, 1000L, 3000L, null);
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        appender.append(1000L, 42.0);
        appender.append(2000L, 43.0);
        memChunk.setChunk(chunk);

        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(memChunk.getChunk());

        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("__name__:test_metric")));
        doc.add(new SortedSetDocValuesField(LABELS, new BytesRef("host:server1")));
        indexWriter.addDocument(doc);
    }
}
