/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.PartitionWindow;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.ResolvedPartition;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.RoutingKey;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link ResolvedPartitions} and related data structures.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Pushdown disabling logic based on routing key distribution across partitions</li>
 *   <li>Fetch-statement-level collision detection</li>
 *   <li>XContent parsing for all ResolvedPartitions structures</li>
 *   <li>Record constructors and null handling</li>
 *   <li>Edge cases (empty partitions, empty routing keys)</li>
 * </ul>
 */
public class ResolvedPartitionsTests extends OpenSearchTestCase {

    /**
     * Test case: Single routing key in single partition
     * Expected: No temporal collision
     */
    public void testSingleRoutingKeyInSinglePartition() {
        RoutingKey serviceApi = new RoutingKey("service", "api");
        PartitionWindow window = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertFalse("No partition overlap when routing key exists in only one partition", resolvedPartitions.hasOverlappingPartitions());
    }

    /**
     * Test case: Same partition_id appears in multiple windows with same routing key
     * Expected: No temporal collision
     *
     * This represents different time windows for the same partition.
     */
    public void testSamePartitionIdWithSameRoutingKeyInMultipleWindows() {
        RoutingKey serviceApi = new RoutingKey("service", "api");

        // Time window 1: 1M-2M
        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));

        // Time window 2: 2M-3M (same partition, different time range)
        PartitionWindow window2 = new PartitionWindow("cluster1:index-a", 2000000L, 3000000L, List.of(serviceApi));

        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window1, window2));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertFalse(
            "No partition overlap when same routing key appears in same partition across different time windows",
            resolvedPartitions.hasOverlappingPartitions()
        );
    }

    /**
     * Test case: Same routing key in different partitions with overlapping time windows
     * Expected: Temporal collision detected
     *
     * This is the actual collision case requiring coordinator aggregation.
     */
    public void testSameRoutingKeyDifferentPartitionsOverlappingTime() {
        RoutingKey serviceApi = new RoutingKey("service", "api");

        // Partition 1: service:api from 1M-2.5M
        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2500000L, List.of(serviceApi));

        // Partition 2: service:api from 2M-3M (overlaps with window1 from 2M-2.5M)
        PartitionWindow window2 = new PartitionWindow("cluster2:index-b", 2000000L, 3000000L, List.of(serviceApi));

        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window1, window2));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertTrue(
            "Partition overlap detected when routing key spans partitions with overlapping times",
            resolvedPartitions.hasOverlappingPartitions()
        );
    }

    /**
     * Test case: Same routing key in different partitions with ADJACENT time windows
     * Expected: No temporal collision
     *
     * This tests the sweep line algorithm's event ordering: when window1 ends at time T
     * and window2 starts at time T, the END event must be processed before the START event.
     * This prevents false collision detection for adjacent (non-overlapping) windows.
     */
    public void testSameRoutingKeyDifferentPartitionsAdjacentTime() {
        RoutingKey serviceApi = new RoutingKey("service", "api");

        // Partition 1: service:api from 1M-2M
        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));

        // Partition 2: service:api from 2M-3M (adjacent, no overlap)
        PartitionWindow window2 = new PartitionWindow("cluster2:index-b", 2000000L, 3000000L, List.of(serviceApi));

        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window1, window2));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertFalse(
            "No partition overlap when routing keys span partitions with adjacent (non-overlapping) time windows",
            resolvedPartitions.hasOverlappingPartitions()
        );
    }

    /**
     * Test case: Empty partition windows
     * Expected: No temporal collision
     */
    public void testEmptyPartitionWindows() {
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of());
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertFalse("No partition overlap when there are no partition windows", resolvedPartitions.hasOverlappingPartitions());
    }

    /**
     * Test case: Empty routing keys in partition windows
     * Expected: No temporal collision
     */
    public void testEmptyRoutingKeys() {
        PartitionWindow window = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of());
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertFalse("No partition overlap when there are no routing keys", resolvedPartitions.hasOverlappingPartitions());
    }

    /**
     * Test case: Different routing keys in different partitions with overlapping time windows
     * Expected: No partition overlap
     *
     * <p>This is a critical test case: temporal overlap alone does NOT cause a collision.
     * A collision only occurs when the SAME routing key exists in multiple partitions
     * during overlapping time windows.</p>
     */
    public void testDifferentRoutingKeysWithTimeOverlap() {
        RoutingKey serviceApi = new RoutingKey("service", "api");
        RoutingKey serviceWeb = new RoutingKey("service", "web");

        // Partition 1: service:api from 1M-2M
        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));

        // Partition 2: service:web from 1.5M-2.5M (overlaps in TIME, but different routing key)
        PartitionWindow window2 = new PartitionWindow("cluster2:index-b", 1500000L, 2500000L, List.of(serviceWeb));

        ResolvedPartition partition = new ResolvedPartition("fetch service:*", List.of(window1, window2));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        assertFalse(
            "No partition overlap when different routing keys exist in partitions despite time overlap",
            resolvedPartitions.hasOverlappingPartitions()
        );
    }

    /**
     * Test multiple fetch statements where one has collision.
     * Expected: Temporal collision detected
     */
    public void testMultipleFetchStatementsWithOneCollision() {
        RoutingKey serviceApi = new RoutingKey("service", "api");
        RoutingKey serviceWeb = new RoutingKey("service", "web");

        // Fetch statement 1: service:api spans multiple partitions (HAS COLLISION)
        PartitionWindow window1a = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));
        PartitionWindow window1b = new PartitionWindow("cluster2:index-b", 1000000L, 2000000L, List.of(serviceApi));
        ResolvedPartition partition1 = new ResolvedPartition("fetch service:api", List.of(window1a, window1b));

        // Fetch statement 2: service:web in single partition (no collision)
        PartitionWindow window2 = new PartitionWindow("cluster3:index-c", 1000000L, 2000000L, List.of(serviceWeb));
        ResolvedPartition partition2 = new ResolvedPartition("fetch service:web", List.of(window2));

        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition1, partition2));

        assertTrue(
            "Partition overlap detected if any fetch statement has routing key collision",
            resolvedPartitions.hasOverlappingPartitions()
        );
    }

    /**
     * Test getAllPartitionIds method
     */
    public void testGetAllPartitionIds() {
        RoutingKey serviceApi = new RoutingKey("service", "api");

        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));
        PartitionWindow window2 = new PartitionWindow("cluster2:index-b", 1000000L, 2000000L, List.of(serviceApi));
        PartitionWindow window3 = new PartitionWindow("cluster1:index-a", 2000000L, 3000000L, List.of(serviceApi)); // Duplicate

        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window1, window2, window3));
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(List.of(partition));

        List<String> partitionIds = resolvedPartitions.getAllPartitionIds();

        assertEquals("Should have 2 unique partition IDs", 2, partitionIds.size());
        assertTrue("Should contain cluster1:index-a", partitionIds.contains("cluster1:index-a"));
        assertTrue("Should contain cluster2:index-b", partitionIds.contains("cluster2:index-b"));
    }

    /**
     * Test null safety in constructors
     */
    public void testNullSafetyInConstructors() {
        // Null partitions list
        ResolvedPartitions resolvedPartitions = new ResolvedPartitions(null);
        assertNotNull(resolvedPartitions.getPartitions());
        assertTrue(resolvedPartitions.getPartitions().isEmpty());

        // Null fetch statement and partition windows
        ResolvedPartition partition = new ResolvedPartition(null, null);
        assertEquals("", partition.getFetchStatement());
        assertNotNull(partition.getPartitionWindows());
        assertTrue(partition.getPartitionWindows().isEmpty());

        // Null partition ID and routing keys
        PartitionWindow window = new PartitionWindow(null, 0L, 0L, null);
        assertEquals("", window.partitionId());
        assertNotNull(window.routingKeys());
        assertTrue(window.routingKeys().isEmpty());

        // Null key and value
        RoutingKey routingKey = new RoutingKey(null, null);
        assertEquals("", routingKey.key());
        assertEquals("", routingKey.value());
    }

    // ========== XContent Parsing Tests ==========

    /**
     * Test parsing complete ResolvedPartitions structure from XContent.
     * This exercises the full parsing hierarchy: ResolvedPartitions -> ResolvedPartition -> PartitionWindow -> RoutingKey.
     */
    public void testParseResolvedPartitionsComplete() throws IOException {
        String json = """
            {
              "partitions": [
                {
                  "fetch_statement": "fetch service:api",
                  "partition_windows": [
                    {
                      "partition_id": "cluster1:index-a",
                      "start": 1000000,
                      "end": 2000000,
                      "routing_keys": [
                        {"key": "service", "value": "api"}
                      ]
                    },
                    {
                      "partition_id": "cluster2:index-b",
                      "start": 1000000,
                      "end": 2000000,
                      "routing_keys": [
                        {"key": "service", "value": "api"}
                      ]
                    }
                  ]
                },
                {
                  "fetch_statement": "fetch service:web",
                  "partition_windows": [
                    {
                      "partition_id": "cluster3:index-c",
                      "start": 1000000,
                      "end": 2000000,
                      "routing_keys": [
                        {"key": "service", "value": "web"}
                      ]
                    }
                  ]
                }
              ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ResolvedPartitions resolvedPartitions = ResolvedPartitions.parse(parser);

            assertNotNull(resolvedPartitions);
            assertEquals(2, resolvedPartitions.getPartitions().size());

            ResolvedPartition partition1 = resolvedPartitions.getPartitions().get(0);
            assertEquals("fetch service:api", partition1.getFetchStatement());
            assertEquals(2, partition1.getPartitionWindows().size());

            ResolvedPartition partition2 = resolvedPartitions.getPartitions().get(1);
            assertEquals("fetch service:web", partition2.getFetchStatement());
            assertEquals(1, partition2.getPartitionWindows().size());
        }
    }

    /**
     * Test parsing empty ResolvedPartitions structure.
     */
    public void testParseResolvedPartitionsEmpty() throws IOException {
        String json = """
            {
              "partitions": []
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ResolvedPartitions resolvedPartitions = ResolvedPartitions.parse(parser);

            assertNotNull(resolvedPartitions);
            assertTrue(resolvedPartitions.getPartitions().isEmpty());
        }
    }

    /**
     * Test parsing partition windows with nil/missing end timestamp.
     */
    public void testParsePartitionWindowsWithMissingEndTimestamp() throws IOException {
        String json = """
            {
              "partitions": [
                {
                  "fetch_statement": "fetch service:k8s-resource-controller",
                  "partition_windows": [
                    {
                      "partition_id": "cluster1:index-2d",
                      "start": 1731003630000,
                      "end": 1731010000000,
                      "routing_keys": [
                        {"key": "region", "value": "dca"}
                      ]
                    },
                    {
                      "partition_id": "cluster2:index-2d",
                      "start": 1731005000000,
                      "routing_keys": [
                        {"key": "region", "value": "dca"}
                      ]
                    }
                  ]
                }
              ]
            }
            """;

        // Use a fixed time supplier for deterministic test behavior
        long fixedNowMs = 1731007000000L;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ResolvedPartitions resolvedPartitions = ResolvedPartitions.parse(parser, () -> fixedNowMs);

            assertNotNull(resolvedPartitions);
            assertEquals(1, resolvedPartitions.getPartitions().size());

            ResolvedPartition partition = resolvedPartitions.getPartitions().get(0);
            assertEquals("fetch service:k8s-resource-controller", partition.getFetchStatement());
            assertEquals(2, partition.getPartitionWindows().size());

            // First window: has explicit end timestamp
            PartitionWindow window1 = partition.getPartitionWindows().get(0);
            assertEquals("cluster1:index-2d", window1.partitionId());
            assertEquals(1731003630000L, window1.startMs());
            assertEquals(1731010000000L, window1.endMs()); // Explicit end value
            assertEquals(1, window1.routingKeys().size());

            // Second window: missing end field, should default to nowMs (our fixed time)
            PartitionWindow window2 = partition.getPartitionWindows().get(1);
            assertEquals("cluster2:index-2d", window2.partitionId());
            assertEquals(1731005000000L, window2.startMs());
            assertEquals(fixedNowMs, window2.endMs()); // Should equal our fixed time
            assertEquals(1, window2.routingKeys().size());

            assertTrue("There should be collision", resolvedPartitions.hasOverlappingPartitions());
        }
    }

    /**
     * Test parsing multiple fetch statements where all partition windows have missing end timestamp.
     */
    public void testMultipleFetchStatementsWithMissingEndTimestamps() throws IOException {
        String json = """
            {
              "partitions": [
                {
                  "fetch_statement": "fetch name:quota_status.requests.memory resources-type:total",
                  "partition_windows": [
                    {
                      "partition_id": "cluster1:opensearch-ts-poc-2-staging-data-dca-tc:2d",
                      "start": 1731003630000,
                      "routing_keys": [
                        {"key": "region", "value": "dca"},
                        {"key": "namespace", "value": "benchmark"}
                      ]
                    }
                  ]
                },
                {
                  "fetch_statement": "fetch name:quota_status.requests.memory resources-type:used",
                  "partition_windows": [
                    {
                      "partition_id": "cluster1:opensearch-ts-poc-2-staging-data-dca-tc:2d",
                      "start": 1731003630000,
                      "routing_keys": [
                        {"key": "region", "value": "dca"},
                        {"key": "namespace", "value": "benchmark"}
                      ]
                    }
                  ]
                }
              ]
            }
            """;

        // Use a fixed time supplier for deterministic test behavior
        long fixedNowMs = 1731010000000L;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ResolvedPartitions resolvedPartitions = ResolvedPartitions.parse(parser, () -> fixedNowMs);

            assertNotNull(resolvedPartitions);
            assertEquals(2, resolvedPartitions.getPartitions().size());

            // First fetch statement
            ResolvedPartition partition1 = resolvedPartitions.getPartitions().get(0);
            assertEquals("fetch name:quota_status.requests.memory resources-type:total", partition1.getFetchStatement());
            assertEquals(1, partition1.getPartitionWindows().size());

            PartitionWindow window1 = partition1.getPartitionWindows().get(0);
            assertEquals("cluster1:opensearch-ts-poc-2-staging-data-dca-tc:2d", window1.partitionId());
            assertEquals(1731003630000L, window1.startMs());
            assertEquals(fixedNowMs, window1.endMs()); // Should equal our fixed time
            assertEquals(2, window1.routingKeys().size());

            // Second fetch statement
            ResolvedPartition partition2 = resolvedPartitions.getPartitions().get(1);
            assertEquals("fetch name:quota_status.requests.memory resources-type:used", partition2.getFetchStatement());
            assertEquals(1, partition2.getPartitionWindows().size());

            PartitionWindow window2 = partition2.getPartitionWindows().get(0);
            assertEquals("cluster1:opensearch-ts-poc-2-staging-data-dca-tc:2d", window2.partitionId());
            assertEquals(1731003630000L, window2.startMs());
            assertEquals(fixedNowMs, window2.endMs()); // Should equal our fixed time
            assertEquals(2, window2.routingKeys().size());

            // Verify both windows use the same reference time (nowMs)
            // They should have the exact same end timestamp since nowMs is captured once
            assertEquals("Both partition windows should use the same nowMs reference time", window1.endMs(), window2.endMs());

            assertFalse("There should be no collision", resolvedPartitions.hasOverlappingPartitions());
        }
    }
}
