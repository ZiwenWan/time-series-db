/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tsdb.query.federation.FederationMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Represents resolved partitions for a federated M3QL query.
 *
 * <p>This class contains information about which partitions (cluster:index pairs)
 * should be queried for different time windows and routing keys.</p>
 *
 * <p>The structure is designed to support federation scenarios where a query
 * may span multiple remote clusters and indices, each potentially containing
 * data for different time ranges and routing key combinations.</p>
 *
 * <p>Implements {@link FederationMetadata} to provide information about partition
 * overlap, which is used to determine query pushdown decisions.</p>
 */
public class ResolvedPartitions implements FederationMetadata {
    private static final String FIELD_PARTITIONS = "partitions";

    private final List<ResolvedPartition> partitions;

    /**
     * Constructs a ResolvedPartitions instance.
     *
     * @param partitions list of resolved partitions
     */
    public ResolvedPartitions(List<ResolvedPartition> partitions) {
        this.partitions = partitions != null ? partitions : List.of();
    }

    /**
     * Gets the list of resolved partitions.
     *
     * @return list of partitions
     */
    public List<ResolvedPartition> getPartitions() {
        return partitions;
    }

    /**
     * Parses ResolvedPartitions from XContent using current system time in milliseconds for missing end timestamps.
     *
     * @param parser the XContent parser positioned at the resolved_partitions object
     * @return parsed ResolvedPartitions instance
     * @throws IOException if parsing fails
     */
    public static ResolvedPartitions parse(XContentParser parser) throws IOException {
        return parse(parser, System::currentTimeMillis);
    }

    /**
     * Parses ResolvedPartitions from XContent with injectable time supplier.
     *
     * <p>This overload allows tests to inject a fixed time supplier for deterministic behavior,
     * avoiding test flakiness from non-monotonic system clock changes.</p>
     *
     * @param parser the XContent parser positioned at the resolved_partitions object
     * @param timeSupplier supplier for current time in milliseconds (used for missing end timestamps)
     * @return parsed ResolvedPartitions instance
     * @throws IOException if parsing fails
     */
    public static ResolvedPartitions parse(XContentParser parser, LongSupplier timeSupplier) throws IOException {
        long nowMs = timeSupplier.getAsLong();

        List<ResolvedPartition> partitions = new ArrayList<>();

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY && FIELD_PARTITIONS.equals(currentFieldName)) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    partitions.add(ResolvedPartition.parse(parser, nowMs));
                }
            }
        }

        return new ResolvedPartitions(partitions);
    }

    /**
     * Checks if any time series data is present across multiple partitions with overlapping time windows.
     *
     * <p>This implements the {@link FederationMetadata} interface to provide information for
     * query pushdown decisions. An overlapping partition scenario occurs when, within any fetch
     * statement, a routing key (key-value pair) appears in more than one partition_id during
     * overlapping time windows, indicating that data for the same series is split across multiple
     * partitions.</p>
     *
     * <p>The overlap check is done per fetch statement, not globally. This allows different
     * fetch statements to have independent collision detection.</p>
     *
     * <p>Example scenario where overlapping partitions exist:
     * <ul>
     *   <li>Fetch statement: "fetch service:api | moving 5m sum"</li>
     *   <li>RoutingKey service:api exists in both cluster1:index-a and cluster2:index-b</li>
     *   <li>Time windows for these partitions overlap</li>
     *   <li>If we push down a moving sum operation to cluster 2,
     *       data present in cluster 1 will not be visible for the 5m moving sum in cluster 2</li>
     *   <li>The computed result would be partial around the boundary</li>
     *   <li>Solution: Perform all aggregations at coordinator level when partitions overlap</li>
     * </ul>
     *
     * @return true if time series data spans multiple partitions with temporal overlap, false otherwise
     */
    @Override
    public boolean hasOverlappingPartitions() {
        // Check for collision per fetch statement (ResolvedPartition) independently
        for (ResolvedPartition partition : partitions) {
            if (hasTemporalCollisionInWindows(partition.getPartitionWindows())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Detects temporal collisions in a list of partition windows using a sweep line algorithm.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Create events for all window start and end times</li>
     *   <li>Sort events by timestamp</li>
     *   <li>Process events in order, maintaining active windows at each time point</li>
     *   <li>At each event, check if any routing key exists in multiple active partitions</li>
     * </ol>
     *
     * @param windows list of partition windows to check
     * @return true if a temporal collision is detected, false otherwise
     */
    private boolean hasTemporalCollisionInWindows(List<PartitionWindow> windows) {
        if (windows.isEmpty()) {
            return false;
        }

        // Create time events (both START and END) for all windows
        List<TimeEvent> events = new ArrayList<>();
        for (PartitionWindow window : windows) {
            events.add(new TimeEvent(window.startMs(), true, window));  // START event
            events.add(new TimeEvent(window.endMs(), false, window));   // END event
        }

        // Sort events by timestamp (END events before START events at same time)
        events.sort((e1, e2) -> {
            int cmp = Long.compare(e1.timestamp, e2.timestamp);
            if (cmp != 0) return cmp;
            // At same timestamp, process END before START to handle adjacent windows correctly
            // For adjacent windows [1, 2) and [2, 3), we want window1 to end before window2 starts
            return Boolean.compare(e1.isStart, e2.isStart);
        });

        // Track active windows at current time point
        Set<PartitionWindow> activeWindows = new HashSet<>();

        // Process events and check for collisions at each time boundary
        for (TimeEvent event : events) {
            if (event.isStart) {
                // Window starts: add to active set
                activeWindows.add(event.window);

                // Check if this creates a collision
                if (hasRoutingKeyCollision(activeWindows)) {
                    return true;
                }
            } else {
                // Window ends: remove from active set
                activeWindows.remove(event.window);
            }
        }

        return false;
    }

    /**
     * Checks if any routing key exists in multiple different partitions among active windows.
     *
     * @param activeWindows currently active partition windows
     * @return true if a routing key spans multiple partitions, false otherwise
     */
    private boolean hasRoutingKeyCollision(Set<PartitionWindow> activeWindows) {
        // Map: routing key -> set of partition IDs where it appears
        Map<String, Set<String>> routingKeyToPartitions = new HashMap<>();

        for (PartitionWindow window : activeWindows) {
            for (RoutingKey routingKey : window.routingKeys()) {
                String routingKeyStr = routingKey.toString();
                routingKeyToPartitions.computeIfAbsent(routingKeyStr, k -> new HashSet<>()).add(window.partitionId());
            }
        }

        // Check if any routing key spans multiple partitions
        return routingKeyToPartitions.values().stream().anyMatch(partitionIds -> partitionIds.size() > 1);
    }

    /**
     * Represents a time event (window start or end) in the sweep line algorithm.
     *
     * @param timestamp the time when the event occurs
     * @param isStart true if this is a window start event, false for end event
     * @param window the partition window associated with this event
     */
    private record TimeEvent(long timestamp, boolean isStart, PartitionWindow window) {
    }

    /**
     * Gets all unique partition IDs across all partition windows.
     *
     * @return list of unique partition IDs
     */
    public List<String> getAllPartitionIds() {
        Set<String> partitionIds = new HashSet<>();
        for (ResolvedPartition partition : partitions) {
            for (PartitionWindow window : partition.getPartitionWindows()) {
                partitionIds.add(window.partitionId());
            }
        }
        return new ArrayList<>(partitionIds);
    }

    /**
     * Represents a single resolved partition with its fetch statement and time windows.
     */
    public static class ResolvedPartition {
        private static final String FIELD_FETCH_STATEMENT = "fetch_statement";
        private static final String FIELD_PARTITION_WINDOWS = "partition_windows";

        private final String fetchStatement;
        private final List<PartitionWindow> partitionWindows;

        /**
         * Constructs a ResolvedPartition instance.
         *
         * @param fetchStatement the M3QL fetch statement for this partition
         * @param partitionWindows list of partition windows with time ranges and routing keys
         */
        public ResolvedPartition(String fetchStatement, List<PartitionWindow> partitionWindows) {
            this.fetchStatement = fetchStatement != null ? fetchStatement : "";
            this.partitionWindows = partitionWindows != null ? partitionWindows : List.of();
        }

        public String getFetchStatement() {
            return fetchStatement;
        }

        public List<PartitionWindow> getPartitionWindows() {
            return partitionWindows;
        }

        /**
         * Parses ResolvedPartition from XContent.
         *
         * @param parser the XContent parser
         * @param nowMs reference time in milliseconds for nil/missing end timestamps
         * @return parsed ResolvedPartition instance
         * @throws IOException if parsing fails
         */
        public static ResolvedPartition parse(XContentParser parser, long nowMs) throws IOException {
            String fetchStatement = null;
            List<PartitionWindow> partitionWindows = new ArrayList<>();

            XContentParser.Token token;
            String currentFieldName = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING && FIELD_FETCH_STATEMENT.equals(currentFieldName)) {
                    fetchStatement = parser.text();
                } else if (token == XContentParser.Token.START_ARRAY && FIELD_PARTITION_WINDOWS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        partitionWindows.add(PartitionWindow.parse(parser, nowMs));
                    }
                }
            }

            return new ResolvedPartition(fetchStatement, partitionWindows);
        }
    }

    /**
     * Represents a time window for a specific partition with associated routing keys.
     *
     * @param partitionId identifier in format "cluster:index"
     * @param startMs start timestamp in milliseconds
     * @param endMs end timestamp in milliseconds
     * @param routingKeys list of routing keys for pushdown optimization
     */
    public record PartitionWindow(String partitionId, long startMs, long endMs, List<RoutingKey> routingKeys) {

        private static final String FIELD_PARTITION_ID = "partition_id";
        private static final String FIELD_START = "start";
        private static final String FIELD_END = "end";
        private static final String FIELD_ROUTING_KEYS = "routing_keys";

        /**
         * Compact constructor with null safety.
         */
        public PartitionWindow {
            partitionId = partitionId != null ? partitionId : "";
            routingKeys = routingKeys != null ? routingKeys : List.of();
        }

        /**
         * Parses PartitionWindow from XContent.
         *
         * @param parser the XContent parser
         * @param nowMs reference time in milliseconds for nil/missing end timestamps
         * @return parsed PartitionWindow instance
         * @throws IOException if parsing fails
         */
        public static PartitionWindow parse(XContentParser parser, long nowMs) throws IOException {
            String partitionId = null;
            long startMs = 0;
            long endMs = nowMs;
            List<RoutingKey> routingKeys = new ArrayList<>();

            XContentParser.Token token;
            String currentFieldName = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING && FIELD_PARTITION_ID.equals(currentFieldName)) {
                    partitionId = parser.text();
                } else if (token == XContentParser.Token.VALUE_NUMBER && FIELD_START.equals(currentFieldName)) {
                    startMs = parser.longValue();
                } else if (token == XContentParser.Token.VALUE_NUMBER && FIELD_END.equals(currentFieldName)) {
                    endMs = parser.longValue();
                } else if (token == XContentParser.Token.START_ARRAY && FIELD_ROUTING_KEYS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        routingKeys.add(RoutingKey.parse(parser));
                    }
                }
            }

            return new PartitionWindow(partitionId, startMs, endMs, routingKeys);
        }
    }

    /**
     * Represents a routing key used for query pushdown optimization.
     *
     * <p>Examples:
     * <ul>
     *   <li>dc:dca60 -> region:dca (federated preprocessing)</li>
     *   <li>service:esdock -> service:esdock (direct mapping)</li>
     * </ul>
     *
     * @param key the routing key name (e.g., "service", "region")
     * @param value the routing key value (preprocessed from federation)
     */
    public record RoutingKey(String key, String value) {
        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        /**
         * Compact constructor with null safety.
         */
        public RoutingKey {
            key = key != null ? key : "";
            value = value != null ? value : "";
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }

        /**
         * Parses RoutingKey from XContent.
         */
        public static RoutingKey parse(XContentParser parser) throws IOException {
            String key = null;
            String value = null;

            XContentParser.Token token;
            String currentFieldName = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (FIELD_KEY.equals(currentFieldName)) {
                        key = parser.text();
                    } else if (FIELD_VALUE.equals(currentFieldName)) {
                        value = parser.text();
                    }
                }
            }

            return new RoutingKey(key, value);
        }
    }
}
