/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.LabelConstants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.MetricsDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility constants and methods for indexing time series data.
 *
 * This class provides common field names and utilities used across the indexing
 * system for time series data, including field names for labels, references,
 * chunks, and timestamps.
 */
public final class IndexUtils {

    /**
     * IndexUtils should not be instantiated
     */
    private IndexUtils() {
        // Utility class
    }

    /**
     * Decode labels from SortedSetDocValues into Labels object for both live series and closed chunk index.
     *
     * @param docId the document ID to extract labels for
     * @param metricsDocValues the metrics doc values containing labels data
     * @return the decoded labels for the document
     * @throws IOException if an error occurs reading the labels
     */
    public static Labels labelsForDoc(int docId, MetricsDocValues metricsDocValues) throws IOException {
        SortedSetDocValues labelsDocValues = metricsDocValues.getLabelsDocValues();
        if (labelsDocValues == null || !labelsDocValues.advanceExact(docId)) {
            return ByteLabels.emptyLabels();
        }

        int valueCount = labelsDocValues.docValueCount();
        // docValueCount() is equivalent to one plus the maximum ordinal, that means ordinal
        // range is [0, docValueCount() - 1]
        List<String> labelStrings = new ArrayList<>(valueCount * 2);

        for (int i = 0; i < valueCount; i++) {
            long ord = labelsDocValues.nextOrd();

            BytesRef term = labelsDocValues.lookupOrd(ord);
            String labelString = term.utf8ToString();

            // Parse "key:value" format (labels are stored with colon separator)
            int delimiterIndex = labelString.indexOf(LabelConstants.LABEL_DELIMITER);
            if (delimiterIndex > 0 && delimiterIndex < labelString.length() - 1) {
                String key = labelString.substring(0, delimiterIndex);
                String value = labelString.substring(delimiterIndex + 1);
                labelStrings.add(key);
                labelStrings.add(value);
            } else {
                // malformed labels
                throw new IOException("Malformed label: " + labelString + " in docId: " + docId);
            }
        }

        // Convert to ByteLabels using the fromStrings method
        return ByteLabels.fromStrings(labelStrings.toArray(new String[0]));
    }

}
