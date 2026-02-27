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
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.tsdb.TSDBPlugin;

import java.io.IOException;
import java.util.Map;

/**
 * Base class for time series aggregation builders that share common time range
 * configuration, validation, and serialization logic.
 *
 * @param <T> The concrete builder type (for fluent API support)
 */
public abstract class AbstractTimeSeriesAggregationBuilder<T extends AbstractTimeSeriesAggregationBuilder<T>> extends
    AbstractAggregationBuilder<T> {

    protected long minTimestamp;
    protected long maxTimestamp;
    protected long step;

    /**
     * Primary constructor with time range validation.
     */
    protected AbstractTimeSeriesAggregationBuilder(String name, long minTimestamp, long maxTimestamp, long step) {
        super(name);
        validateTimeRange(minTimestamp, maxTimestamp, step);
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    /**
     * StreamInput constructor. Subclass must call {@link #readTimeRange(StreamInput)}
     * at the appropriate point in its own deserialization sequence.
     */
    protected AbstractTimeSeriesAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Copy constructor.
     */
    protected AbstractTimeSeriesAggregationBuilder(
        AbstractTimeSeriesAggregationBuilder<T> clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.minTimestamp = clone.minTimestamp;
        this.maxTimestamp = clone.maxTimestamp;
        this.step = clone.step;
    }

    /**
     * Read time range fields from a stream. Called by subclass at the correct
     * point in its deserialization sequence to preserve wire format.
     */
    protected void readTimeRange(StreamInput in) throws IOException {
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.step = in.readLong();
    }

    /**
     * Write time range fields to a stream. Called by subclass at the correct
     * point in its serialization sequence to preserve wire format.
     */
    protected void writeTimeRange(StreamOutput out) throws IOException {
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeLong(step);
    }

    private static void validateTimeRange(long minTimestamp, long maxTimestamp, long step) {
        if (maxTimestamp <= minTimestamp) {
            throw new IllegalArgumentException(
                "maxTimestamp must be greater than minTimestamp (minTimestamp=" + minTimestamp + ", maxTimestamp=" + maxTimestamp + ")"
            );
        }
        if (step <= 0) {
            throw new IllegalArgumentException("step must be positive, got: " + step);
        }
    }

    /**
     * Validate that TSDB engine is enabled on the target index.
     */
    protected void validateTsdbEnabled(QueryShardContext queryShardContext) {
        boolean tsdbEnabled = TSDBPlugin.TSDB_ENGINE_ENABLED.get(queryShardContext.getIndexSettings().getSettings());
        if (!tsdbEnabled) {
            throw new IllegalStateException("Time Series aggregator can only be used on indices where index.tsdb_engine.enabled is true");
        }
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getStep() {
        return step;
    }

    /**
     * Compare time range fields for equality. Subclasses should call this
     * from their {@code equals()} implementation.
     */
    protected boolean timeRangeEquals(AbstractTimeSeriesAggregationBuilder<?> other) {
        return minTimestamp == other.minTimestamp && maxTimestamp == other.maxTimestamp && step == other.step;
    }

    /**
     * Compute hash code contribution from time range fields. Subclasses should
     * incorporate this into their {@code hashCode()} implementation.
     */
    protected int timeRangeHashCode() {
        int result = Long.hashCode(minTimestamp);
        result = 31 * result + Long.hashCode(maxTimestamp);
        result = 31 * result + Long.hashCode(step);
        return result;
    }
}
