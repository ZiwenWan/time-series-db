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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.utils.SampleMerger;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opensearch.tsdb.query.utils.RamUsageConstants;
import org.opensearch.tsdb.query.breaker.ReduceCircuitBreakerConsumer;

/**
 * Internal aggregation result for time series pipeline aggregators.
 *
 * <p>This class represents the result of time series pipeline aggregations, containing
 * a collection of time series data that can be processed through various pipeline stages.
 * It implements the {@link TimeSeriesProvider} interface to provide access to the
 * underlying time series data.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Time Series Storage:</strong> Maintains a list of time series with their
 *       associated samples, labels, and metadata</li>
 *   <li><strong>Reduce Stage Support:</strong> Supports optional reduce stages for
 *       final aggregation operations</li>
 *   <li><strong>Label-based Merging:</strong> Uses {@link SampleMerger} for
 *       intelligent merging of time series with matching labels</li>
 *   <li><strong>Serialization:</strong> Supports streaming serialization/deserialization
 *       for distributed processing</li>
 * </ul>
 *
 * <h2>Wire Format Versions:</h2>
 * <ul>
 *   <li><strong>V0 (Legacy):</strong> Per-sample serialization, no sentinel byte</li>
 *   <li><strong>V1:</strong> Delta-encoded SampleList, sentinel -1</li>
 *   <li><strong>V2:</strong> V1 + exec-stats longs appended after reduce stage, sentinel -2</li>
 * </ul>
 *
 * <h2>Usage Pattern:</h2>
 * <p>This class is typically created by time series aggregators to represent their results.
 * The time series data can then be further processed through pipeline stages or returned as
 * final results.</p>
 */
public class InternalTimeSeries extends InternalAggregation implements TimeSeriesProvider {

    private final List<TimeSeries> timeSeries;
    private final UnaryPipelineStage reduceStage;
    private final AggregationExecStats execStats;
    private final AggregationDataSource dataSource;
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

    public static final int VERSION_0 = 0;
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;
    public static final Set<Integer> SUPPORTED_VERSIONS = Set.of(VERSION_0, VERSION_1, VERSION_2);

    public static volatile int serialFormatSetting = VERSION_0; // this will be synced with the cluster setting

    /**
     * Creates a new InternalTimeSeries aggregation result without a reduce stage.
     *
     * @param name the name of the aggregation
     * @param timeSeries the list of time series data
     * @param metadata the aggregation metadata
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeries, Map<String, Object> metadata) {
        this(name, timeSeries, metadata, null, AggregationExecStats.EMPTY, AggregationDataSource.EMPTY);
    }

    /**
     * Creates a new InternalTimeSeries aggregation result with an optional reduce stage.
     *
     * @param name the name of the aggregation
     * @param timeSeries the list of time series data
     * @param metadata the aggregation metadata
     * @param reduceStage the optional reduce stage for final aggregation operations
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeries, Map<String, Object> metadata, UnaryPipelineStage reduceStage) {
        this(name, timeSeries, metadata, reduceStage, AggregationExecStats.EMPTY, AggregationDataSource.EMPTY);
    }

    /**
     * Creates a new InternalTimeSeries aggregation result with an optional reduce stage, execution stats, and data source.
     *
     * @param name the name of the aggregation
     * @param timeSeries the list of time series data
     * @param metadata the aggregation metadata
     * @param reduceStage the optional reduce stage for final aggregation operations
     * @param execStats the execution stats snapshot for this shard result (use {@link AggregationExecStats#EMPTY} when not needed)
     * @param dataSource the data source metadata (use {@link AggregationDataSource#EMPTY} when not needed)
     */
    public InternalTimeSeries(
        String name,
        List<TimeSeries> timeSeries,
        Map<String, Object> metadata,
        UnaryPipelineStage reduceStage,
        AggregationExecStats execStats,
        AggregationDataSource dataSource
    ) {
        super(name, metadata);
        this.timeSeries = timeSeries;
        this.reduceStage = reduceStage;
        this.execStats = execStats != null ? execStats : AggregationExecStats.EMPTY;
        this.dataSource = dataSource != null ? dataSource : AggregationDataSource.EMPTY;
    }

    /**
     * Reads an InternalTimeSeries from a stream for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public InternalTimeSeries(StreamInput in) throws IOException {
        super(in);
        // Read time series
        int timeSeriesCount = in.readVInt();
        int serialVersion = resolveSerialVersion(timeSeriesCount);
        // V1+: Read extra int for seriesCount when version >=1
        if (serialVersion >= VERSION_1) {
            timeSeriesCount = in.readVInt();
        }
        this.timeSeries = new ArrayList<>(timeSeriesCount);
        for (int i = 0; i < timeSeriesCount; i++) {
            this.timeSeries.add(readTimeSeries(in, serialVersion));
        }

        // Read the reduce stage information
        boolean hasReduceStage = in.readBoolean();
        if (hasReduceStage) {
            String stageName = in.readString();
            this.reduceStage = (UnaryPipelineStage) PipelineStageFactory.readFrom(in, stageName);
        } else {
            this.reduceStage = null;
        }

        // V2+: read exec stats and data source after reduce stage
        if (serialVersion >= VERSION_2) {
            this.execStats = new AggregationExecStats(in);
            this.dataSource = new AggregationDataSource(in);
        } else {
            this.execStats = AggregationExecStats.EMPTY;
            this.dataSource = AggregationDataSource.EMPTY;
        }
    }

    /**
     * Resolves the wire format version from the first VInt read off the stream.
     * A non-negative value is the actual time series count (V0 legacy format),
     * while a negative value encodes the version as {@code -version} (e.g. -1 for V1, -2 for V2).
     *
     * @param timeSeriesCount the first VInt read from the stream
     * @return the resolved serial version
     * @throws IllegalStateException if the encoded version is not in {@link #SUPPORTED_VERSIONS}
     */
    private static int resolveSerialVersion(int timeSeriesCount) {
        if (timeSeriesCount >= 0) {
            return VERSION_0;
        }
        int version = -timeSeriesCount;
        if (!SUPPORTED_VERSIONS.contains(version)) {
            throw new IllegalStateException("Unknown serial version: " + version + ". Supported versions: " + SUPPORTED_VERSIONS);
        }
        return version;
    }

    /**
     * Writes the InternalTimeSeries data to a stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        if (serialFormatSetting == VERSION_0) {
            legacyWriteTo(out);
            return;
        }
        out.writeVInt(-serialFormatSetting);
        out.writeVInt(timeSeries.size());
        for (TimeSeries series : timeSeries) {
            out.writeInt(0); // hash - placeholder for now
            SampleList.writeTo(series.getSamples(), out);

            // Write labels - convert to map for serialization
            Map<String, String> labelsMap = series.getLabels() != null ? series.getLabels().toMapView() : new HashMap<>();
            out.writeMap(labelsMap, StreamOutput::writeString, StreamOutput::writeString);

            // Write alias
            out.writeOptionalString(series.getAlias());

            // Write TimeSeries metadata
            out.writeLong(series.getMinTimestamp());
            out.writeLong(series.getMaxTimestamp());
            out.writeLong(series.getStep());
        }

        // Write the reduce stage information
        if (reduceStage != null) {
            out.writeBoolean(true);
            out.writeString(reduceStage.getName());
            reduceStage.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        // V2+: write exec stats and data source after reduce stage
        if (serialFormatSetting >= VERSION_2) {
            execStats.writeTo(out);
            dataSource.writeTo(out);
        }
    }

    private void legacyWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(timeSeries.size());
        for (TimeSeries series : timeSeries) {
            out.writeInt(0); // hash - placeholder for now
            SampleList samples = series.getSamples();
            out.writeVInt(samples.size());
            for (Sample sample : samples) {
                sample.writeTo(out);
            }

            // Write labels - convert to map for serialization
            Map<String, String> labelsMap = series.getLabels() != null ? series.getLabels().toMapView() : new HashMap<>();
            out.writeMap(labelsMap, StreamOutput::writeString, StreamOutput::writeString);

            // Write alias
            out.writeOptionalString(series.getAlias());

            // Write TimeSeries metadata
            out.writeLong(series.getMinTimestamp());
            out.writeLong(series.getMaxTimestamp());
            out.writeLong(series.getStep());
        }

        // Write the reduce stage information
        if (reduceStage != null) {
            out.writeBoolean(true);
            out.writeString(reduceStage.getName());
            reduceStage.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Returns the writeable name used for stream serialization.
     *
     * @return the writeable name "time_series"
     */
    @Override
    public String getWriteableName() {
        return "time_series";
    }

    /**
     * Reduces multiple InternalTimeSeries aggregations into a single result.
     *
     * <p>This method handles two scenarios:</p>
     * <ul>
     * <li><strong>With reduce stage:</strong> Delegates to the stage's reduce method</li>
     * <li><strong>Without reduce stage:</strong> Merges time series by labels using {@link SampleMerger}</li>
     * </ul>
     *
     * <p>Circuit breaker tracking is performed to protect coordinator nodes (including
     * data cluster coordinators in CCS setups) from OOM conditions.</p>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     * @throws IllegalArgumentException if any aggregation is not a TimeSeriesProvider
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        try (ReduceCircuitBreakerConsumer cbConsumer = ReduceCircuitBreakerConsumer.createConsumer(reduceContext)) {
            // Merge exec stats from all input InternalTimeSeries across both code paths
            AggregationExecStats mergedStats = aggregations.stream()
                .filter(a -> a instanceof InternalTimeSeries)
                .map(a -> ((InternalTimeSeries) a).getExecStats())
                .reduce(AggregationExecStats.EMPTY, AggregationExecStats::merge);

            // Merge data source metadata from all input InternalTimeSeries across both code paths
            AggregationDataSource mergedDataSource = aggregations.stream()
                .filter(a -> a instanceof InternalTimeSeries)
                .map(a -> ((InternalTimeSeries) a).getDataSource())
                .reduce(AggregationDataSource.EMPTY, AggregationDataSource::merge);

            // If we have a reduce stage, delegate directly to it (skip merging)
            if (reduceStage != null) {
                // Track ArrayList allocation for providers list
                cbConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

                // Convert aggregations to TimeSeriesProvider list for the stage's reduce method
                List<TimeSeriesProvider> timeSeriesProviders = new ArrayList<>(aggregations.size());
                for (InternalAggregation agg : aggregations) {
                    if (!(agg instanceof TimeSeriesProvider)) {
                        throw new IllegalArgumentException("aggregation: " + agg + " is not a TimeSeriesProvider");
                    }
                    timeSeriesProviders.add((TimeSeriesProvider) agg);
                }

                // Use the stage's own reduce method with circuit breaker tracking
                InternalAggregation result = reduceStage.reduce(timeSeriesProviders, reduceContext.isFinalReduce(), cbConsumer);
                // Propagate merged exec stats to the result
                if (result instanceof InternalTimeSeries its) {
                    return new InternalTimeSeries(its.name, its.timeSeries, its.metadata, its.reduceStage, mergedStats, mergedDataSource);
                }
                return result;
            }

            // No reduce stage - collect all time series from all aggregations and merge by labels
            // Track HashMap base overhead
            cbConsumer.accept(RamUsageConstants.HASHMAP_SHALLOW_SIZE);

            Map<Labels, TimeSeries> mergedSeriesByLabels = new HashMap<>();

            for (InternalAggregation aggregation : aggregations) {
                if (!(aggregation instanceof TimeSeriesProvider)) {
                    throw new IllegalArgumentException("aggregation: " + aggregation + " is not a TimeSeriesProvider");
                }
                TimeSeriesProvider provider = (TimeSeriesProvider) aggregation;
                List<TimeSeries> timeSeries = provider.getTimeSeries();

                for (TimeSeries series : timeSeries) {
                    // Use direct Labels comparison for better performance (no string conversion)
                    Labels seriesLabels = series.getLabels();

                    TimeSeries existingSeries = mergedSeriesByLabels.get(seriesLabels);
                    if (existingSeries != null) {
                        // Merge samples from same time series across segments using helper
                        // Use assumeSorted=true for reduce operations as samples should be sorted
                        SampleList mergedSamples = MERGE_HELPER.merge(
                            existingSeries.getSamples(),
                            series.getSamples(),
                            true // assumeSorted - samples should be sorted in reduce phase
                        );

                        // Track merged samples memory
                        cbConsumer.accept(mergedSamples.ramBytesUsed());

                        // Create new merged time series (reuse existing labels and metadata)
                        TimeSeries mergedSeries = new TimeSeries(
                            mergedSamples,
                            existingSeries.getLabels(),
                            existingSeries.getMinTimestamp(),
                            existingSeries.getMaxTimestamp(),
                            existingSeries.getStep(),
                            existingSeries.getAlias()
                        );
                        mergedSeriesByLabels.put(seriesLabels, mergedSeries);
                    } else {
                        // First occurrence of this time series - track HashMap entry + full series (labels + samples)
                        cbConsumer.accept(RamUsageConstants.groupEntryBaseOverhead(seriesLabels) + series.ramBytesUsed());
                        mergedSeriesByLabels.put(seriesLabels, series);
                    }
                }
            }

            // Track result ArrayList allocation
            cbConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

            List<TimeSeries> combinedTimeSeries = new ArrayList<>(mergedSeriesByLabels.values());

            // Return combined time series (no reduce stage), with merged exec stats
            return new InternalTimeSeries(name, combinedTimeSeries, metadata, null, mergedStats, mergedDataSource);
        }
    }

    /**
     * Retrieves a property value based on the given path.
     *
     * <p>Supported properties:</p>
     * <ul>
     *   <li><strong>Empty path:</strong> Returns this aggregation instance</li>
     *   <li><strong>"timeSeries":</strong> Returns the list of time series data</li>
     * </ul>
     *
     * @param path the property path to retrieve
     * @return the property value
     * @throws IllegalArgumentException if the property path is unknown
     */
    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String property = path.get(0);
            if ("timeSeries".equals(property)) {
                return timeSeries;
            }
        }
        throw new IllegalArgumentException("Unknown property [" + path.get(0) + "] for TimeSeriesUnfoldAggregation [" + name + "]");
    }

    /**
     * Returns the list of time series contained in this aggregation result.
     *
     * @return the list of time series data
     */
    public List<TimeSeries> getTimeSeries() {
        return timeSeries;
    }

    /**
     * Gets the reduce stage associated with this aggregation result.
     *
     * @return the reduce stage, or null if no reduce stage is set
     */
    public UnaryPipelineStage getReduceStage() {
        return reduceStage;
    }

    /**
     * Creates a new TimeSeriesProvider with the given time series data.
     *
     * <p>This method is used to create a reduced aggregation result with
     * new time series data while preserving the original name, metadata,
     * and reduce stage configuration.</p>
     *
     * @param timeSeries the new time series data
     * @return a new InternalTimeSeries instance with the provided data
     */
    @Override
    public TimeSeriesProvider createReduced(List<TimeSeries> timeSeries) {
        return new InternalTimeSeries(name, timeSeries, metadata, reduceStage, execStats, dataSource);
    }

    /**
     * Returns the aggregated execution stats carried by this result.
     * On the coordinator, this contains the sum of shard-level stats from all contributing shards.
     *
     * @return the execution stats; never null (falls back to {@link AggregationExecStats#EMPTY})
     */
    public AggregationExecStats getExecStats() {
        return execStats;
    }

    /**
     * Returns the data source metadata carried by this result.
     * On the coordinator, this contains the merged metadata from all contributing shards.
     *
     * @return the data source metadata; never null (falls back to {@link AggregationDataSource#EMPTY})
     */
    public AggregationDataSource getDataSource() {
        return dataSource;
    }

    /**
     * Serializes the time series data to XContent format.
     *
     * <p>The output includes:</p>
     * <ul>
     * <li>Time series array with samples, labels, and metadata</li>
     * <li>Individual sample timestamps and values</li>
     * <li>Series aliases, min/max timestamps, and step information</li>
     * </ul>
     *
     * @param builder the XContent builder to write to
     * @param params the serialization parameters
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("timeSeries");
        for (TimeSeries series : timeSeries) {
            builder.startObject();
            builder.field("hash", 0); // placeholder for now
            if (series.getAlias() != null) {
                builder.field("alias", series.getAlias());
            }
            builder.field("minTimestamp", series.getMinTimestamp());
            builder.field("maxTimestamp", series.getMaxTimestamp());
            builder.field("step", series.getStep());
            builder.startArray("samples");
            for (Sample sample : series.getSamples()) {
                builder.startObject();
                builder.field("timestamp", sample.getTimestamp());
                builder.field("value", sample.getValue());
                builder.endObject();
            }
            builder.endArray();
            // Include labels information in XContent if available
            if (series.getLabels() != null && !series.getLabels().isEmpty()) {
                builder.field("labels", series.getLabels().toMapView());
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    /**
     * Indicates whether this aggregation must be reduced even when there's only
     * a single internal aggregation.
     *
     * @return false, as InternalTimeSeries does not require reduction for single aggregations
     */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    /**
     * Reads a TimeSeries object from a stream input during deserialization.
     *
     * @param in the stream input to read from
     * @param serialVersion the wire format version
     * @return the deserialized TimeSeries object
     * @throws IOException if an I/O error occurs during reading
     */
    private static TimeSeries readTimeSeries(StreamInput in, int serialVersion) throws IOException {
        if (serialVersion == VERSION_0) {
            return readTimeSeriesLegacy(in);
        }
        int hash = in.readInt();
        SampleList samples = SampleList.readFrom(in);

        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        Labels labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);

        String alias = in.readOptionalString();

        // Read TimeSeries metadata
        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();

        return new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    private static TimeSeries readTimeSeriesLegacy(StreamInput in) throws IOException {
        int hash = in.readInt();
        int sampleCount = in.readVInt();
        List<Sample> samples = new ArrayList<>(sampleCount);

        for (int i = 0; i < sampleCount; i++) {
            samples.add(Sample.readFrom(in));
        }

        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        Labels labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);

        String alias = in.readOptionalString();

        // Read TimeSeries metadata
        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();

        return new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTimeSeries that = (InternalTimeSeries) o;
        // execStats and dataSource are intentionally excluded: they are side-channel diagnostics, not part of result
        // identity. Two InternalTimeSeries with identical time-series data but different exec stats
        // (e.g. collected from different cluster topologies) represent the same query result and
        // must compare equal.
        return Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && timeSeriesListEquals(timeSeries, that.timeSeries)
            && Objects.equals(
                reduceStage != null ? reduceStage.getName() : null,
                that.reduceStage != null ? that.reduceStage.getName() : null
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            getName(),
            getMetadata(),
            timeSeriesListHashCode(timeSeries),
            reduceStage != null ? reduceStage.getName() : null
        );
    }

    /**
     * Compare two time series lists for equality.
     */
    private boolean timeSeriesListEquals(List<TimeSeries> list1, List<TimeSeries> list2) {
        if (list1 == list2) return true;
        if (list1 == null || list2 == null) return false;
        if (list1.size() != list2.size()) return false;

        for (int i = 0; i < list1.size(); i++) {
            TimeSeries ts1 = list1.get(i);
            TimeSeries ts2 = list2.get(i);

            // Compare key fields
            if (!Objects.equals(ts1.getAlias(), ts2.getAlias())) return false;
            if (ts1.getMinTimestamp() != ts2.getMinTimestamp()) return false;
            if (ts1.getMaxTimestamp() != ts2.getMaxTimestamp()) return false;
            if (ts1.getStep() != ts2.getStep()) return false;
            if (!ts1.getLabels().toMapView().equals(ts2.getLabels().toMapView())) return false;
            if (ts1.getSamples().size() != ts2.getSamples().size()) return false;
        }
        return true;
    }

    /**
     * Compute hash code for time series list.
     */
    private int timeSeriesListHashCode(List<TimeSeries> list) {
        if (list == null) return 0;
        int result = 1;
        for (TimeSeries ts : list) {
            result = 31 * result + (ts == null
                ? 0
                : Objects.hash(ts.getAlias(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getSamples().size()));
        }
        return result;
    }
}
