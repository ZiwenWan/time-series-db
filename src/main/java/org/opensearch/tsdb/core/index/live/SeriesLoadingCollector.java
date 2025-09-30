/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene collector for loading complete series data.
 *
 * This collector loads full series information from the live series index,
 * including all series data and metadata needed for query processing and
 * result construction.
 */
public class SeriesLoadingCollector implements Collector {

    private final SeriesLoader seriesLoader;
    private NumericDocValues referenceValues;
    private SortedSetDocValues labelsValues;
    private long maxReference = 0L;

    /**
     * Constructs a SeriesLoadingCollector with the given head instance.
     * @param seriesLoader SeriesLoadingCallback, used to load series
     */
    public SeriesLoadingCollector(SeriesLoader seriesLoader) {
        this.seriesLoader = seriesLoader;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
        referenceValues = leafReaderContext.reader().getNumericDocValues(Constants.IndexSchema.REFERENCE);
        labelsValues = leafReaderContext.reader().getSortedSetDocValues(Constants.IndexSchema.LABELS);

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // no scoring needed
            }

            @Override
            public void collect(int doc) throws IOException {
                assert labelsValues instanceof SortedSetDocValues : "Label creation requires sorted input given by SortedSetDocValues";

                labelsValues.advanceExact(doc);
                List<String> keyValuePairs = new ArrayList<>();
                for (int i = 0; i < labelsValues.docValueCount(); i++) {
                    keyValuePairs.add(labelsValues.lookupOrd(labelsValues.nextOrd()).utf8ToString());
                }
                Labels labels = ByteLabels.fromSortedKeyValuePairs(keyValuePairs); // SortedSetDocValues returns sorted values

                referenceValues.advanceExact(doc);
                long reference = referenceValues.longValue();

                seriesLoader.load(new MemSeries(reference, labels));
                if (maxReference < reference) {
                    maxReference = reference;
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * Get the max reference seen
     * @return the max reference
     */
    public long getMaxReference() {
        return maxReference;
    }
}
