/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.index.IndexUtils;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.MetricsDocValues;
import org.opensearch.tsdb.core.reader.MetricsLeafReader;

import java.io.IOException;
import java.util.List;

/**
 * MetricsLeafReader implementation for ClosedChunkIndex segments.
 *
 * This reader provides access to closed (completed) time series chunks that have been
 * persisted to disk. Each document contains a single serialized chunk that is decoded
 * on demand during query processing.
 */
public class ClosedChunkIndexLeafReader extends MetricsLeafReader {

    private final LeafReader inner;

    /**
     * Constructs a ClosedChunkIndexLeafReader for accessing closed chunk data.
     *
     * @param inner the underlying LeafReader to wrap
     * @throws IOException if an error occurs during initialization
     */
    public ClosedChunkIndexLeafReader(LeafReader inner) throws IOException {
        super(inner);
        this.inner = inner;
    }

    @Override
    public MetricsDocValues getMetricsDocValues() throws IOException {
        try {
            BinaryDocValues chunkValues = this.getBinaryDocValues(Constants.IndexSchema.CHUNK);
            SortedSetDocValues labelsDocValues = this.getSortedSetDocValues(Constants.IndexSchema.LABELS);
            if (chunkValues == null) {
                throw new IOException("Chunk field '" + Constants.IndexSchema.CHUNK + "'  not found in index.");
            }
            if (labelsDocValues == null) {
                throw new IOException("Labels field '" + Constants.IndexSchema.LABELS + "' not found in index.");
            }
            return new ClosedChunkIndexMetricsDocValues(chunkValues, labelsDocValues);
        } catch (IOException e) {
            throw new IOException("Error accessing MetricsDocValues in ClosedChunkIndexLeafReader: " + e.getMessage(), e);
        }
    }

    @Override
    public List<ChunkIterator> chunksForDoc(int docId, MetricsDocValues metricsDocValues) throws IOException {

        BinaryDocValues chunkValues = metricsDocValues.getChunkDocValues();
        if (!chunkValues.advanceExact(docId)) {
            throw new IOException("Chunk field 'chunk' not found for document in closed chunk index.");
        }

        BytesRef chunkBytes = chunkValues.binaryValue();
        if (chunkBytes == null || chunkBytes.length == 0) {
            return List.of();
        }

        // Decode the serialized chunk
        ClosedChunk closedChunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(chunkValues.binaryValue());
        return List.of(closedChunk.getChunkIterator());
    }

    @Override
    public Labels labelsForDoc(int docId, MetricsDocValues metricsDocValues) throws IOException {
        return IndexUtils.labelsForDoc(docId, metricsDocValues);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return inner.getCoreCacheHelper();
    }

    @Override
    public Terms terms(String s) throws IOException {
        return inner.terms(s);
    }

    @Override
    public NumericDocValues getNumericDocValues(String s) throws IOException {
        return inner.getNumericDocValues(s);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String s) throws IOException {
        return inner.getBinaryDocValues(s);
    }

    @Override
    public SortedDocValues getSortedDocValues(String s) throws IOException {
        return inner.getSortedDocValues(s);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String s) throws IOException {
        return inner.getSortedNumericDocValues(s);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String s) throws IOException {
        return inner.getSortedSetDocValues(s);
    }

    @Override
    public NumericDocValues getNormValues(String s) throws IOException {
        return inner.getNormValues(s);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String s) throws IOException {
        return inner.getDocValuesSkipper(s);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String s) throws IOException {
        return inner.getFloatVectorValues(s);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String s) throws IOException {
        return inner.getByteVectorValues(s);
    }

    @Override
    public void searchNearestVectors(String s, float[] floats, KnnCollector knnCollector, Bits bits) throws IOException {
        inner.searchNearestVectors(s, floats, knnCollector, bits);
    }

    @Override
    public void searchNearestVectors(String s, byte[] bytes, KnnCollector knnCollector, Bits bits) throws IOException {
        inner.searchNearestVectors(s, bytes, knnCollector, bits);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return inner.getFieldInfos();
    }

    @Override
    public Bits getLiveDocs() {
        return inner.getLiveDocs();
    }

    @Override
    public PointValues getPointValues(String s) throws IOException {
        return inner.getPointValues(s);
    }

    @Override
    public void checkIntegrity() throws IOException {
        inner.checkIntegrity();
    }

    @Override
    public LeafMetaData getMetaData() {
        return inner.getMetaData();
    }

    @Override
    public TermVectors termVectors() throws IOException {
        return inner.termVectors();
    }

    @Override
    public int numDocs() {
        return inner.numDocs();
    }

    @Override
    public int maxDoc() {
        return inner.maxDoc();
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return inner.storedFields();
    }

    @Override
    protected void doClose() throws IOException {
        inner.close();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return inner.getReaderCacheHelper();
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
