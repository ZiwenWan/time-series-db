/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.tsdb.core.reader.MetricsDocValues;

/**
 * LiveSeriesIndexMetricsDocValues is a wrapper class for holding chunk reference doc values and labels doc values for live series index.
 */
public class LiveSeriesIndexMetricsDocValues extends MetricsDocValues {

    /**
     * Constructor for live series index metrics doc values.
     *
     * @param chunkRefDocValues the numeric doc values containing chunk references
     * @param labelsDocValues the sorted set doc values containing labels
     */
    public LiveSeriesIndexMetricsDocValues(NumericDocValues chunkRefDocValues, SortedSetDocValues labelsDocValues) {
        super(chunkRefDocValues, labelsDocValues);
    }

    @Override
    public NumericDocValues getChunkRefDocValues() {
        return this.chunkRefDocValues;
    }

    @Override
    public BinaryDocValues getChunkDocValues() {
        throw new UnsupportedOperationException("Live Series Index does not support chunk doc values");
    }
}
