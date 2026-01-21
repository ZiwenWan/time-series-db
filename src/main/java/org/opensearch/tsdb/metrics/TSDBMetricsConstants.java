/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

/** Metric names, descriptions, and units. */
public final class TSDBMetricsConstants {

    private TSDBMetricsConstants() {
        // Utility class, no instantiation
    }

    // ============================================
    // Engine Metrics (Ingestion, Lifecycle, Flush)
    // ============================================

    // Ingestion Counters
    /** Counter: Total number of samples ingested into TSDB across all shards */
    public static final String SAMPLES_INGESTED_TOTAL = "tsdb.samples.ingested.total";

    /** Counter: Total number of time series created across all shards */
    public static final String SERIES_CREATED_TOTAL = "tsdb.series.created.total";

    /** Counter: Total number of memory chunks created across all shards */
    public static final String MEMCHUNKS_CREATED_TOTAL = "tsdb.memchunks.created.total";

    /** Counter: Total number of out-of-order samples rejected across all shards */
    public static final String OOO_SAMPLES_REJECTED_TOTAL = "tsdb.ooo_samples.rejected.total";

    /** Counter: Total number of out-of-order chunks created across all shards */
    public static final String OOO_CHUNKS_CREATED_TOTAL = "tsdb.ooo_chunks.created.total";

    /** Counter: Total number of out-of-order chunks merged across all shards */
    public static final String OOO_CHUNKS_MERGED_TOTAL = "tsdb.ooo_chunks.merged.total";

    // Lifecycle Counters
    /** Counter: Total number of series closed (e.g., due to inactivity) */
    public static final String SERIES_CLOSED_TOTAL = "tsdb.series.closed.total";

    /** Counter: Total number of in-memory chunks expired (e.g., due to inactivity) */
    public static final String MEMCHUNKS_EXPIRED_TOTAL = "tsdb.memchunks.expired.total";

    /** Counter: Total number of memory chunks closed and flushed to disk */
    public static final String MEMCHUNKS_CLOSED_TOTAL = "tsdb.memchunks.closed.total";

    // Snapshot Histograms (Gauge-like metrics)
    /** Histogram: Current number of open series in head (recorded on flush) */
    public static final String SERIES_OPEN = "tsdb.series.open";

    /** Histogram: Current number of open in-memory chunks in head (recorded on flush) */
    public static final String MEMCHUNKS_OPEN = "tsdb.memchunks.open";

    /** Histogram: Minimum sequence number among open in-memory chunks (recorded on flush) */
    public static final String MEMCHUNKS_MINSEQ = "tsdb.memchunks.minseq";

    /** Histogram: Size histogram (bytes) of closed chunks */
    public static final String CLOSEDCHUNKS_SIZE = "tsdb.closedchunks.size";

    /** Histogram: Latency of flush operation */
    public static final String FLUSH_LATENCY = "tsdb.flush.latency";

    /** Counter: Total number of commits (closeHeadChunks + commitSegmentInfos) */
    public static final String COMMIT_TOTAL = "tsdb.commit.total";

    // ============================================
    // Aggregation Metrics (Query/Read Path)
    // ============================================

    /** Histogram: Latency of collect() operation per request */
    public static final String AGGREGATION_COLLECT_LATENCY = "tsdb.aggregation.collect.latency";

    /** Histogram: Latency of postCollect() operation per request */
    public static final String AGGREGATION_POST_COLLECT_LATENCY = "tsdb.aggregation.post_collect.latency";

    /** Histogram: Total Lucene documents processed per request */
    public static final String AGGREGATION_DOCS_TOTAL = "tsdb.aggregation.docs.total";

    /** Histogram: Live index documents processed per request */
    public static final String AGGREGATION_DOCS_LIVE = "tsdb.aggregation.docs.live";

    /** Histogram: Closed chunk index documents processed per request */
    public static final String AGGREGATION_DOCS_CLOSED = "tsdb.aggregation.docs.closed";

    /** Histogram: Total chunks processed per request */
    public static final String AGGREGATION_CHUNKS_TOTAL = "tsdb.aggregation.chunks.total";

    /** Histogram: Live chunks processed per request */
    public static final String AGGREGATION_CHUNKS_LIVE = "tsdb.aggregation.chunks.live";

    /** Histogram: Closed chunks processed per request */
    public static final String AGGREGATION_CHUNKS_CLOSED = "tsdb.aggregation.chunks.closed";

    /** Histogram: Total samples processed per request */
    public static final String AGGREGATION_SAMPLES_TOTAL = "tsdb.aggregation.samples.total";

    /** Histogram: Live samples processed per request */
    public static final String AGGREGATION_SAMPLES_LIVE = "tsdb.aggregation.samples.live";

    /** Histogram: Closed samples processed per request */
    public static final String AGGREGATION_SAMPLES_CLOSED = "tsdb.aggregation.samples.closed";

    /** Counter: Total errors in chunksForDoc() operations */
    public static final String AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL = "tsdb.aggregation.chunks_for_doc.errors.total";

    /** Counter: Total query results (tagged with status: empty or hits) */
    public static final String AGGREGATION_RESULTS_TOTAL = "tsdb.aggregation.results.total";

    /** Histogram: Number of time series returned per query */
    public static final String AGGREGATION_SERIES_TOTAL = "tsdb.aggregation.series.total";

    // ============================================
    // Search Metrics (Query Cache)
    // ============================================

    /** Counter: Total wildcard query cache hits */
    public static final String SEARCH_WILDCARD_CACHE_HITS_TOTAL = "tsdb.search.wildcard_cache.hits.total";

    /** Counter: Total wildcard query cache misses */
    public static final String SEARCH_WILDCARD_CACHE_MISSES_TOTAL = "tsdb.search.wildcard_cache.misses.total";

    /** Counter: Total wildcard query cache evictions */
    public static final String SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL = "tsdb.search.wildcard_cache.evictions.total";

    /** Histogram: Current wildcard query cache size (number of entries) */
    public static final String SEARCH_WILDCARD_CACHE_SIZE = "tsdb.search.wildcard_cache.size";

    // ============================================
    // Refresh/Visibility Metrics
    // ============================================

    /** Histogram: Time between NRT refreshes (new series visibility lag) */
    public static final String REFRESH_INTERVAL = "tsdb.refresh.interval";

    // ============================================
    // Reader Metrics (Document Limits, Capacity)
    // ============================================

    /** Gauge: Percentage of Lucene's document limit used (0-100+) */
    public static final String READER_MAXDOC_UTILIZATION = "tsdb.reader.maxdoc.utilization";

    /** Gauge: Number of closed chunk indices currently loaded */
    public static final String READER_CLOSED_INDICES = "tsdb.reader.closed_indices";

    /** Gauge: Number of leaf readers in TSDBDirectoryReader */
    public static final String READER_LEAF_COUNT = "tsdb.reader.leaf_count";

    // ============================================
    // Index Metrics (Retention, Compaction)
    // ============================================

    // Counters
    /** Counter: Total number of closed chunk indexes created */
    public static final String INDEX_CREATED_TOTAL = "tsdb.index.created.total";

    /** Counter: Total number of indexes deleted by retention */
    public static final String RETENTION_SUCCESS_TOTAL = "tsdb.retention.success.total";

    /** Counter: Total number of failed retention deletions */
    public static final String RETENTION_FAILURE_TOTAL = "tsdb.retention.failure.total";

    /** Counter: Total number of successful compactions */
    public static final String COMPACTION_SUCCESS_TOTAL = "tsdb.compaction.success.total";

    /** Counter: Total number of failed compactions */
    public static final String COMPACTION_FAILURE_TOTAL = "tsdb.compaction.failure.total";

    /** Counter: Total number of indexes deleted by compaction */
    public static final String COMPACTION_DELETED_TOTAL = "tsdb.compaction.deleted.total";

    // Histograms
    /** Histogram: Total size (bytes) of all closed chunk indexes */
    public static final String INDEX_SIZE = "tsdb.index.size";

    /** Histogram: Age (ms) of online indexes (first to last) */
    public static final String INDEX_ONLINE_AGE = "tsdb.index.online.age";

    /** Histogram: Age (ms) of indexes pending closure (offline) */
    public static final String INDEX_OFFLINE_AGE = "tsdb.index.offline.age";

    /** Histogram: Latency (ms) of retention operations */
    public static final String RETENTION_LATENCY = "tsdb.retention.latency";

    /** Histogram: Configured retention period (ms) */
    public static final String RETENTION_AGE = "tsdb.retention.age";

    /** Histogram: Latency (ms) of compaction operations */
    public static final String COMPACTION_LATENCY = "tsdb.compaction.latency";

    // ============================================
    // Metric Descriptions
    // ============================================

    // Engine Metrics - Ingestion
    public static final String SAMPLES_INGESTED_TOTAL_DESC = "Total number of samples ingested into TSDB across all shards";
    public static final String SERIES_CREATED_TOTAL_DESC = "Total number of time series created across all shards";
    public static final String MEMCHUNKS_CREATED_TOTAL_DESC = "Total number of memory chunks created across all shards";
    public static final String OOO_SAMPLES_REJECTED_TOTAL_DESC = "Total number of out-of-order samples rejected across all shards";
    public static final String OOO_CHUNKS_CREATED_TOTAL_DESC = "Total number of out-of-order chunks created across all shards";
    public static final String OOO_CHUNKS_MERGED_TOTAL_DESC = "Total number of out-of-order chunks merged across all shards";

    // Engine Metrics - Lifecycle
    public static final String SERIES_CLOSED_TOTAL_DESC = "Total number of series closed (e.g., due to inactivity)";
    public static final String MEMCHUNKS_EXPIRED_TOTAL_DESC = "Total number of in-memory chunks expired (e.g., due to inactivity)";
    public static final String MEMCHUNKS_CLOSED_TOTAL_DESC = "Total number of memory chunks closed and flushed to disk";

    // Engine Metrics - Snapshots
    public static final String SERIES_OPEN_DESC = "Current number of open series in head (recorded on flush)";
    public static final String MEMCHUNKS_OPEN_DESC = "Current number of open in-memory chunks in head (recorded on flush)";
    public static final String MEMCHUNKS_MINSEQ_DESC = "Minimum sequence number among open in-memory chunks (recorded on flush)";
    public static final String CLOSEDCHUNKS_SIZE_DESC = "Size histogram (bytes) of closed chunks persisted to disk";
    public static final String FLUSH_LATENCY_DESC = "Latency of flush operation";
    public static final String COMMIT_TOTAL_DESC = "Total number of commits (closeHeadChunks + commitSegmentInfos)";

    // Aggregation Metrics
    public static final String AGGREGATION_COLLECT_LATENCY_DESC = "Latency of collect() operation per aggregation request";
    public static final String AGGREGATION_POST_COLLECT_LATENCY_DESC = "Latency of postCollect() operation per aggregation request";
    public static final String AGGREGATION_DOCS_TOTAL_DESC = "Total Lucene documents processed per aggregation request";
    public static final String AGGREGATION_DOCS_LIVE_DESC = "Live index documents processed per aggregation request";
    public static final String AGGREGATION_DOCS_CLOSED_DESC = "Closed chunk index documents processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_TOTAL_DESC = "Total chunks processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_LIVE_DESC = "Live chunks processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_CLOSED_DESC = "Closed chunks processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_TOTAL_DESC = "Total samples processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_LIVE_DESC = "Live samples processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_CLOSED_DESC = "Closed samples processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC = "Total errors in chunksForDoc() operations";
    public static final String AGGREGATION_RESULTS_TOTAL_DESC = "Total queries tagged by result status (empty or hits)";
    public static final String AGGREGATION_SERIES_TOTAL_DESC = "Number of time series returned per query";

    // Search Metrics
    public static final String SEARCH_WILDCARD_CACHE_HITS_TOTAL_DESC = "Total wildcard query cache hits";
    public static final String SEARCH_WILDCARD_CACHE_MISSES_TOTAL_DESC = "Total wildcard query cache misses";
    public static final String SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL_DESC = "Total wildcard query cache evictions";
    public static final String SEARCH_WILDCARD_CACHE_SIZE_DESC = "Current wildcard query cache size (number of entries)";

    // Refresh/Visibility Metrics
    public static final String REFRESH_INTERVAL_DESC = "Time between NRT refreshes (new series visibility lag)";

    // Reader Metrics
    public static final String READER_MAXDOC_UTILIZATION_DESC = "Percentage of Lucene's document limit used (0-100+)";
    public static final String READER_CLOSED_INDICES_DESC = "Number of closed chunk indices currently loaded";
    public static final String READER_LEAF_COUNT_DESC = "Number of leaf readers in TSDBDirectoryReader";

    // Index Metrics
    public static final String INDEX_CREATED_TOTAL_DESC = "Total number of closed chunk indexes created";
    public static final String INDEX_SIZE_DESC = "Total size (bytes) of all closed chunk indexes";
    public static final String INDEX_ONLINE_AGE_DESC = "Age (ms) of online indexes (first to last)";
    public static final String INDEX_OFFLINE_AGE_DESC = "Age (ms) of indexes pending closure (offline)";
    public static final String RETENTION_SUCCESS_TOTAL_DESC = "Total number of indexes deleted by retention";
    public static final String RETENTION_FAILURE_TOTAL_DESC = "Total number of failed retention deletions";
    public static final String RETENTION_LATENCY_DESC = "Latency (ms) of retention operations";
    public static final String RETENTION_AGE_DESC = "Configured retention period (ms)";
    public static final String COMPACTION_SUCCESS_TOTAL_DESC = "Total number of successful compactions";
    public static final String COMPACTION_FAILURE_TOTAL_DESC = "Total number of failed compactions";
    public static final String COMPACTION_LATENCY_DESC = "Latency (ms) of compaction operations";
    public static final String COMPACTION_DELETED_TOTAL_DESC = "Total number of indexes deleted by compaction";

    // ============================================
    // Metric Tags
    // ============================================

    /** Tag key for result status */
    public static final String TAG_STATUS = "status";

    /** Tag value for empty results */
    public static final String TAG_STATUS_EMPTY = "empty";

    /** Tag value for results with hits */
    public static final String TAG_STATUS_HITS = "hits";

    // ============================================
    // Metric Units
    // ============================================

    /** Unit for dimensionless counts */
    public static final String UNIT_COUNT = "1";

    /** Unit for milliseconds */
    public static final String UNIT_MILLISECONDS = "ms";

    /** Unit for bytes */
    public static final String UNIT_BYTES = "bytes";
}
