/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

/**
 * Callback interface for updating series information.
 * <p>
 * This interface defines a method for updating series data based on a reference identifier.
 */
public interface SeriesUpdater {

    /**
     * Updates the series information for the given series reference.
     * @param reference the reference of the series to update
     * @param maxSeqNo the maxSeqNo to set for the series
     */
    void update(long reference, long maxSeqNo);
}
