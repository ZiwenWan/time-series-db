/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for streaming aggregation state that processes documents incrementally.
 */
interface StreamingAggregationState {
    void processDocument(int docId, TSDBDocValues docValues, TSDBLeafReader reader) throws IOException;

    List<TimeSeries> getFinalResults(long minTimestamp, long maxTimestamp, long step);

    long getEstimatedMemoryUsage();

    boolean hasData();
}

/**
 * Time-indexed arrays for a single group in streaming aggregation.
 */
class GroupTimeArrays {
    private final double[] values;
    private final int[] counts; // Only used for AVG
    private boolean hasData = false;
    private int nonNanCount = 0;

    GroupTimeArrays(int size, boolean needsCounts) {
        this.values = new double[size];
        this.counts = needsCounts ? new int[size] : null;
        Arrays.fill(values, Double.NaN);
    }

    int size() {
        return values.length;
    }

    void aggregate(int timeIndex, double value, StreamingAggregationType type) {
        boolean wasNaN = Double.isNaN(values[timeIndex]);
        switch (type) {
            case SUM:
                values[timeIndex] = wasNaN ? value : values[timeIndex] + value;
                break;
            case MIN:
                values[timeIndex] = wasNaN ? value : Math.min(values[timeIndex], value);
                break;
            case MAX:
                values[timeIndex] = wasNaN ? value : Math.max(values[timeIndex], value);
                break;
            case AVG:
                values[timeIndex] = wasNaN ? value : values[timeIndex] + value;
                counts[timeIndex]++;
                break;
        }
        if (wasNaN) {
            nonNanCount++;
        }
        hasData = true;
    }

    SampleList createSampleList(long minTimestamp, long step, StreamingAggregationType type) {
        if (!hasData) {
            return SampleList.fromList(Collections.emptyList());
        }

        // AVG uses SumCountSample, which is not a simple float sample
        if (type == StreamingAggregationType.AVG) {
            List<Sample> samples = new ArrayList<>(nonNanCount);
            for (int i = 0; i < values.length; i++) {
                if (!Double.isNaN(values[i]) && counts[i] > 0) {
                    long timestamp = minTimestamp + (i * step);
                    samples.add(new SumCountSample(timestamp, values[i], counts[i]));
                }
            }
            return SampleList.fromList(samples);
        }

        FloatSampleList.Builder builder = new FloatSampleList.Builder(nonNanCount);
        for (int i = 0; i < values.length; i++) {
            if (!Double.isNaN(values[i])) {
                long timestamp = minTimestamp + (i * step);
                builder.add(timestamp, values[i]);
            }
        }
        return builder.build();
    }

    boolean hasData() {
        return hasData;
    }

    /**
     * Find the start index in a sample list using binary search.
     */
    static int findStartIndex(SampleList samples, long minTimestamp) {
        int index = samples.search(minTimestamp);
        return index >= 0 ? index : -(index + 1);
    }

    long getEstimatedMemoryUsage() {
        long size = values.length * 8; // double array
        if (counts != null) {
            size += counts.length * 4; // int array
        }
        return size + 32; // object overhead
    }
}

/**
 * Streaming state implementation for no-tag aggregations (single time series result).
 */
class NoTagStreamingState implements StreamingAggregationState {
    private final StreamingAggregationType aggregationType;
    private final GroupTimeArrays arrays;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    NoTagStreamingState(StreamingAggregationType aggregationType, int timeArraySize, long minTimestamp, long maxTimestamp, long step) {
        this.aggregationType = aggregationType;
        this.arrays = new GroupTimeArrays(timeArraySize, aggregationType.requiresCountTracking());
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    @Override
    public void processDocument(int docId, TSDBDocValues docValues, TSDBLeafReader reader) throws IOException {
        // Skip label reading entirely for no-tag case - maximum performance optimization
        List<ChunkIterator> chunks = reader.chunksForDoc(docId, docValues);

        for (ChunkIterator chunk : chunks) {
            processChunk(chunk);
        }
    }

    private void processChunk(ChunkIterator chunk) throws IOException {
        SampleList samples = chunk.decodeSamples(this.minTimestamp, this.maxTimestamp).samples();
        int startIndex = GroupTimeArrays.findStartIndex(samples, this.minTimestamp);

        for (int i = startIndex; i < samples.size(); i++) {
            double value = samples.getValue(i);
            // Skip NaN samples during collection
            if (Double.isNaN(value)) {
                continue;
            }

            // Calculate time index for direct array access
            int timeIndex = (int) ((samples.getTimestamp(i) - minTimestamp) / step);
            if (timeIndex >= 0 && timeIndex < arrays.size()) {
                arrays.aggregate(timeIndex, value, aggregationType);
            }
        }
    }

    @Override
    public List<TimeSeries> getFinalResults(long minTimestamp, long maxTimestamp, long step) {
        if (!arrays.hasData()) {
            return Collections.emptyList();
        }

        SampleList sampleList = arrays.createSampleList(minTimestamp, step, aggregationType);
        if (sampleList.isEmpty()) {
            return Collections.emptyList();
        }

        // Create single TimeSeries with empty labels (global aggregation)
        TimeSeries timeSeries = new TimeSeries(sampleList, ByteLabels.emptyLabels(), this.minTimestamp, this.maxTimestamp, this.step, null);

        return Collections.singletonList(timeSeries);
    }

    @Override
    public long getEstimatedMemoryUsage() {
        return arrays.getEstimatedMemoryUsage() + 64; // object overhead
    }

    @Override
    public boolean hasData() {
        return arrays.hasData();
    }
}

/**
 * Streaming state implementation for tag-based aggregations (grouped time series results).
 */
class TagStreamingState implements StreamingAggregationState {
    private final StreamingAggregationType aggregationType;
    private final List<String> groupByTags;
    private final Map<ByteLabels, GroupTimeArrays> groupData = new HashMap<>();
    private final int timeArraySize;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    TagStreamingState(
        StreamingAggregationType aggregationType,
        List<String> groupByTags,
        int timeArraySize,
        long minTimestamp,
        long maxTimestamp,
        long step
    ) {
        this.aggregationType = aggregationType;
        this.groupByTags = groupByTags;
        this.timeArraySize = timeArraySize;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    @Override
    public void processDocument(int docId, TSDBDocValues docValues, TSDBLeafReader reader) throws IOException {
        // Read only required group-by labels
        Labels allLabels = reader.labelsForDoc(docId, docValues);
        ByteLabels groupLabels = extractGroupLabels(allLabels);

        if (groupLabels == null) {
            // Document missing required group-by tags, skip it
            return;
        }

        // Get or create group arrays for this label combination
        GroupTimeArrays arrays = groupData.computeIfAbsent(
            groupLabels,
            k -> new GroupTimeArrays(timeArraySize, aggregationType.requiresCountTracking())
        );

        // Process chunks for this group
        List<ChunkIterator> chunks = reader.chunksForDoc(docId, docValues);
        for (ChunkIterator chunk : chunks) {
            processChunkForGroup(chunk, arrays);
        }
    }

    private ByteLabels extractGroupLabels(Labels allLabels) {
        if (groupByTags.isEmpty()) {
            return ByteLabels.emptyLabels();
        }

        Map<String, String> groupLabelMap = new HashMap<>();
        for (String tagName : groupByTags) {
            if (allLabels != null && allLabels.has(tagName)) {
                groupLabelMap.put(tagName, allLabels.get(tagName));
            } else {
                return null;
            }
        }

        return ByteLabels.fromMap(groupLabelMap);
    }

    private void processChunkForGroup(ChunkIterator chunk, GroupTimeArrays arrays) throws IOException {
        SampleList samples = chunk.decodeSamples(this.minTimestamp, this.maxTimestamp).samples();
        int startIndex = GroupTimeArrays.findStartIndex(samples, this.minTimestamp);

        for (int i = startIndex; i < samples.size(); i++) {
            double value = samples.getValue(i);
            if (Double.isNaN(value)) {
                continue;
            }

            int timeIndex = (int) ((samples.getTimestamp(i) - minTimestamp) / step);
            if (timeIndex >= 0 && timeIndex < timeArraySize) {
                arrays.aggregate(timeIndex, value, aggregationType);
            }
        }
    }

    @Override
    public List<TimeSeries> getFinalResults(long minTimestamp, long maxTimestamp, long step) {
        List<TimeSeries> results = new ArrayList<>();

        for (Map.Entry<ByteLabels, GroupTimeArrays> entry : groupData.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            GroupTimeArrays arrays = entry.getValue();

            SampleList sampleList = arrays.createSampleList(minTimestamp, step, aggregationType);
            if (!sampleList.isEmpty()) {
                TimeSeries timeSeries = new TimeSeries(
                    sampleList,
                    groupLabels,
                    this.minTimestamp,
                    this.maxTimestamp,
                    this.step,
                    null // No alias for streaming aggregation
                );
                results.add(timeSeries);
            }
        }

        return results;
    }

    @Override
    public long getEstimatedMemoryUsage() {
        long totalSize = 64; // object overhead
        for (GroupTimeArrays arrays : groupData.values()) {
            totalSize += arrays.getEstimatedMemoryUsage();
        }
        return totalSize;
    }

    @Override
    public boolean hasData() {
        return !groupData.isEmpty();
    }
}
