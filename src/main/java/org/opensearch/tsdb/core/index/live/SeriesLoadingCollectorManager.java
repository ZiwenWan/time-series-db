/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.search.CollectorManager;

import java.util.Collection;

/**
 * Manager for creating and coordinating series loading collectors.
 *
 * This collector manager creates SeriesLoadingCollector instances for loading
 * complete series data from the live series index during query execution,
 * handling parallel collection across multiple index segments.
 */
public class SeriesLoadingCollectorManager implements CollectorManager<SeriesLoadingCollector, Long> {
    private final SeriesLoader seriesLoader;

    /**
     * Constructor for SeriesLoadingCollectorManager
     * @param seriesLoader SeriesLoader to load series with
     */
    public SeriesLoadingCollectorManager(SeriesLoader seriesLoader) {
        this.seriesLoader = seriesLoader;
    }

    @Override
    public SeriesLoadingCollector newCollector() {
        return new SeriesLoadingCollector(seriesLoader);
    }

    @Override
    public Long reduce(Collection<SeriesLoadingCollector> collectors) {
        return collectors.stream().mapToLong(SeriesLoadingCollector::getMaxReference).max().orElse(0L);
    }
}
