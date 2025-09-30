/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.opensearch.tsdb.core.head.MemSeries;

/**
 * Callback interface for loading series data for a MemSeries.
 */
public interface SeriesLoader {

    /**
     * Load series data for the given MemSeries.
     * @param series MemSeries to load data for
     */
    void load(MemSeries series);
}
