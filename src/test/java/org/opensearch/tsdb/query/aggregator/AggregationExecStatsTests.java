/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class AggregationExecStatsTests extends OpenSearchTestCase {

    public void testConstructorAndAccessors() {
        AggregationExecStats stats = new AggregationExecStats(10L, 20L, 30L, 40L, 50L, 60L, 70L);

        assertEquals(10L, stats.seriesNumInput());
        assertEquals(20L, stats.samplesNumInput());
        assertEquals(30L, stats.chunksNumClosed());
        assertEquals(40L, stats.chunksNumLive());
        assertEquals(50L, stats.docsNumClosed());
        assertEquals(60L, stats.docsNumLive());
        assertEquals(70L, stats.memoryBytes());
    }

    public void testEmpty() {
        AggregationExecStats empty = AggregationExecStats.EMPTY;

        assertEquals(0L, empty.seriesNumInput());
        assertEquals(0L, empty.samplesNumInput());
        assertEquals(0L, empty.chunksNumClosed());
        assertEquals(0L, empty.chunksNumLive());
        assertEquals(0L, empty.docsNumClosed());
        assertEquals(0L, empty.docsNumLive());
        assertEquals(0L, empty.memoryBytes());
    }

    public void testWriteToAndReadFrom() throws IOException {
        AggregationExecStats original = new AggregationExecStats(100L, 200L, 300L, 400L, 500L, 600L, 700L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        AggregationExecStats deserialized = new AggregationExecStats(in);

        assertEquals(original, deserialized);
    }

    public void testMerge() {
        AggregationExecStats stats1 = new AggregationExecStats(10L, 20L, 30L, 40L, 50L, 60L, 70L);
        AggregationExecStats stats2 = new AggregationExecStats(1L, 2L, 3L, 4L, 5L, 6L, 7L);

        AggregationExecStats merged = stats1.merge(stats2);

        assertEquals(11L, merged.seriesNumInput());
        assertEquals(22L, merged.samplesNumInput());
        assertEquals(33L, merged.chunksNumClosed());
        assertEquals(44L, merged.chunksNumLive());
        assertEquals(55L, merged.docsNumClosed());
        assertEquals(66L, merged.docsNumLive());
        assertEquals(77L, merged.memoryBytes());
    }

    public void testMergeWithEmpty() {
        AggregationExecStats stats = new AggregationExecStats(5L, 10L, 15L, 20L, 25L, 30L, 35L);

        AggregationExecStats mergedLeft = AggregationExecStats.EMPTY.merge(stats);
        assertEquals(stats, mergedLeft);

        AggregationExecStats mergedRight = stats.merge(AggregationExecStats.EMPTY);
        assertEquals(stats, mergedRight);
    }
}
