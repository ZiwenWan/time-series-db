/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * A wrapper class for holding different DocValues types for time series index so that DocValues can be used in same thread that acquired them.
 */
public abstract class MetricsDocValues {
    /** The numeric doc values containing chunk references */
    protected NumericDocValues chunkRefDocValues;
    /** The binary doc values containing serialized chunk data */
    protected BinaryDocValues chunkDocValues;
    /** The sorted set doc values containing labels data */
    SortedSetDocValues labelsDocValues;

    /**
     * Constructor for metrics doc values with chunk reference doc values.
     *
     * @param chunkRefDocValues the numeric doc values containing chunk references
     * @param labelsDocValues the sorted set doc values containing labels
     */
    public MetricsDocValues(NumericDocValues chunkRefDocValues, SortedSetDocValues labelsDocValues) {
        this.chunkDocValues = null;
        this.chunkRefDocValues = chunkRefDocValues;
        this.labelsDocValues = labelsDocValues;
    }

    /**
     * Constructor for metrics doc values with binary chunk data.
     *
     * @param chunkDocValues the binary doc values containing serialized chunk data
     * @param labelsDocValues the sorted set doc values containing labels
     */
    public MetricsDocValues(BinaryDocValues chunkDocValues, SortedSetDocValues labelsDocValues) {
        this.chunkRefDocValues = null;
        this.chunkDocValues = chunkDocValues;
        this.labelsDocValues = labelsDocValues;
    }

    /**
     * Gets the numeric doc values containing chunk references.
     *
     * @return the chunk reference doc values
     */
    public abstract NumericDocValues getChunkRefDocValues();

    /**
     * Gets the binary doc values containing serialized chunk data.
     *
     * @return the chunk doc values
     */
    public abstract BinaryDocValues getChunkDocValues();

    /**
     * Gets the sorted set doc values containing labels.
     *
     * @return the labels doc values
     */
    public SortedSetDocValues getLabelsDocValues() {
        return labelsDocValues;
    }
}
