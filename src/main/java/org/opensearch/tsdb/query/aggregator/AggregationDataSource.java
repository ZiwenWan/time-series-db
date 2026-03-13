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
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable value record carrying data-source metadata for one {@link InternalTimeSeries} aggregation result.
 *
 * <p>Fields describe the origin system(s) and index/resolution metadata of the queried time series.
 * During {@link InternalTimeSeries#reduce}, instances from different shards are merged via
 * {@link #merge(AggregationDataSource)} which unions origins and deduplicates indexes by
 * {@code (index, stepSize)} pair.</p>
 *
 * @param origins  the origin system names (e.g. "prometheus", "graphite")
 * @param indexes  the index/resolution metadata entries
 */
public record AggregationDataSource(Set<String> origins, Set<IndexInfo> indexes) implements Writeable {

    /**
     * Describes a single index and its associated step size (resolution).
     *
     * @param index    the index name or retention alias (e.g. "2d", "30d")
     * @param stepSize the step size string (e.g. "10s", "1m")
     */
    public record IndexInfo(String index, String stepSize) implements Writeable {

        /**
         * Deserializing constructor.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during reading
         */
        public IndexInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(stepSize);
        }
    }

    /** Zero-value constant used when data source info is not available. */
    public static final AggregationDataSource EMPTY = new AggregationDataSource(Set.of(), Set.of());

    /**
     * Deserializing constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public AggregationDataSource(StreamInput in) throws IOException {
        this(new LinkedHashSet<>(in.readStringList()), readIndexInfoSet(in));
    }

    private static Set<IndexInfo> readIndexInfoSet(StreamInput in) throws IOException {
        int size = in.readVInt();
        Set<IndexInfo> set = new LinkedHashSet<>(size);
        for (int i = 0; i < size; i++) {
            set.add(new IndexInfo(in));
        }
        return set;
    }

    /**
     * Serializes this instance to a stream.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(origins);
        out.writeVInt(indexes.size());
        for (IndexInfo idx : indexes) {
            idx.writeTo(out);
        }
    }

    /**
     * Returns a new {@code AggregationDataSource} whose fields are the union of
     * {@code this} and {@code other}. Origins are deduplicated preserving insertion order.
     * IndexInfo entries are deduplicated by {@code (index, stepSize)} pair.
     * Used during {@link InternalTimeSeries#reduce} to accumulate metadata across shards.
     *
     * @param other the other data source to merge with; must not be null
     * @return a new merged instance
     */
    public AggregationDataSource merge(AggregationDataSource other) {
        Objects.requireNonNull(other, "other must not be null");

        Set<String> mergedOrigins = new LinkedHashSet<>(this.origins);
        mergedOrigins.addAll(other.origins);

        Set<IndexInfo> mergedIndexes = new LinkedHashSet<>(this.indexes);
        mergedIndexes.addAll(other.indexes);

        return new AggregationDataSource(mergedOrigins, mergedIndexes);
    }
}
