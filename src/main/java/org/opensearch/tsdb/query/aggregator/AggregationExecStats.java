/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Immutable value record carrying summed execution stats for one {@link InternalTimeSeries} aggregation result.
 *
 * <p>All fields are shard-level counters that are summed across shards during
 * {@link InternalTimeSeries#reduce(java.util.List, org.opensearch.search.aggregations.InternalAggregation.ReduceContext)}.
 * The "numOutput" fields (series output count and sample output count) are NOT included here because they
 * reflect the final coordinator result and are computed in the response listener from the actual
 * {@link InternalTimeSeries} result.</p>
 *
 * <p>Wire format: 7 consecutive {@code writeLong}/{@code readLong} calls. No version guard —
 * coordinated cluster restart is required when upgrading the plugin.</p>
 */
public record AggregationExecStats(long seriesNumInput,   // data.series.numInput — raw series read from storage per shard
    long samplesNumInput,  // data.samples.numInput — samples after shard-level filter/pipeline stages
    long chunksNumClosed,  // storage.chunks.closed
    long chunksNumLive,    // storage.chunks.live
    long docsNumClosed,    // storage.documents.closed
    long docsNumLive,      // storage.documents.live
    long memoryBytes       // resource.memoryBytes — maxCircuitBreakerBytes
) implements Writeable {

    /** Zero-value constant used when exec stats are not requested. */
    public static final AggregationExecStats EMPTY = new AggregationExecStats(0, 0, 0, 0, 0, 0, 0);

    /**
     * Deserializing constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public AggregationExecStats(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong());
    }

    /**
     * Serializes this instance to a stream.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(seriesNumInput);
        out.writeLong(samplesNumInput);
        out.writeLong(chunksNumClosed);
        out.writeLong(chunksNumLive);
        out.writeLong(docsNumClosed);
        out.writeLong(docsNumLive);
        out.writeLong(memoryBytes);
    }

    /**
     * Returns a new {@code AggregationExecStats} whose fields are the element-wise sum of
     * {@code this} and {@code other}. Used during {@link InternalTimeSeries#reduce} to
     * accumulate stats across shards.
     *
     * @param other the other stats to merge with; must not be null
     * @return a new merged instance
     */
    public AggregationExecStats merge(AggregationExecStats other) {
        Objects.requireNonNull(other, "other must not be null");
        return new AggregationExecStats(
            this.seriesNumInput + other.seriesNumInput,
            this.samplesNumInput + other.samplesNumInput,
            this.chunksNumClosed + other.chunksNumClosed,
            this.chunksNumLive + other.chunksNumLive,
            this.docsNumClosed + other.docsNumClosed,
            this.docsNumLive + other.docsNumLive,
            this.memoryBytes + other.memoryBytes
        );
    }
}
