/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.node.NodeClosedException;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for TSDB replica recovery scenarios.
 * Tests peer recovery from primary to replica shards.
 *
 * <p>Replica recovery occurs when:
 * <ul>
 *   <li>A new replica shard is added to the cluster</li>
 *   <li>A replica recovers data from its primary shard</li>
 *   <li>Number of replicas is increased</li>
 * </ul>
 *
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TSDBReplicaRecoveryIT extends TSDBRecoveryITBase {

    private static final String INDEX_NAME = "recovery_test_index";
    private static final int SHARD_COUNT = 1;

    /**
     * Tests basic replica recovery from primary shard.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testReplicaRecovery()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Start node A with primary shard only</li>
     *   <li>Index time series samples</li>
     *   <li>Start node B and add replica</li>
     *   <li>Validate replica recovers from primary</li>
     * </ol>
     */
    public void testReplicaRecovery() throws Exception {
        logger.info("--> starting node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> creating TSDB index with 0 replicas");
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, SHARD_COUNT, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        // Index samples
        int sampleCount = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(sampleCount, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, INDEX_NAME);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        logger.info("--> indexed {} samples on primary", sampleCount);

        // Start second node and add replica
        logger.info("--> starting node B");
        final String nodeB = internalCluster().startNode();

        logger.info("--> adding replica to trigger peer recovery");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen(INDEX_NAME);

        logger.info("--> verifying recovery response");
        final RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        // Should have 2 shards total (1 primary + 1 replica)
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat("Should have 2 recovery states (primary + replica)", recoveryStates.size(), equalTo(2));

        // Find recovery states for each node
        List<RecoveryState> nodeAStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat("Node A should have 1 recovery state", nodeAStates.size(), equalTo(1));

        List<RecoveryState> nodeBStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat("Node B should have 1 recovery state", nodeBStates.size(), equalTo(1));

        // Validate node A recovery (primary initialization)
        final RecoveryState nodeAState = nodeAStates.get(0);
        assertRecoveryState(nodeAState, 0, RecoverySource.EmptyStoreRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, null, nodeA);

        // Validate node B recovery (replica from primary)
        final RecoveryState nodeBState = nodeBStates.get(0);
        assertRecoveryState(nodeBState, 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeA, nodeB);

        validateTSDBRecovery(INDEX_NAME, 0, baseTimestamp, samplesIntervalMillis, sampleCount);
    }

    /**
     * Find recovery states for a specific target node.
     *
     * @param nodeName The target node name
     * @param recoveryStates All recovery states
     * @return List of recovery states for the target node
     */
    private List<RecoveryState> findRecoveriesForTargetNode(String nodeName, List<RecoveryState> recoveryStates) {
        return recoveryStates.stream()
            .filter(state -> state.getTargetNode() != null && state.getTargetNode().getName().equals(nodeName))
            .toList();
    }

    /**
     * Tests removing and re-adding replica.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testRepeatedRecovery()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with replica and index initial samples</li>
     *   <li>Remove replica (set replicas to 0)</li>
     *   <li>Index more samples on primary only</li>
     *   <li>Re-add replica (set replicas to 1)</li>
     *   <li>Verify replica successfully recovers all data</li>
     * </ol>
     */
    public void testRepeatedRecovery() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        final String indexName = "test-index";
        IndexConfig indexConfig = createDefaultIndexConfig(indexName, 1, 1);
        createTimeSeriesIndex(indexConfig);

        // Index some time-series samples
        int initialSampleCount = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        if (initialSampleCount > 0) {
            List<TimeSeriesSample> samples = generateTimeSeriesSamples(initialSampleCount, baseTimestamp, samplesIntervalMillis);
            ingestSamples(samples, indexName);
        }
        client().admin().indices().prepareFlush(indexName).setForce(true).get();

        assertThat(client().admin().indices().prepareFlush(indexName).get().getFailedShards(), equalTo(0));

        logger.info("--> remove replicas");
        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("index.number_of_replicas", 0))
        );
        ensureGreen(indexName);

        logger.info("--> index more time-series samples");
        int additionalSampleCount = randomIntBetween(0, 10);
        if (additionalSampleCount > 0) {
            List<TimeSeriesSample> moreSamples = generateTimeSeriesSamples(
                additionalSampleCount,
                baseTimestamp + (initialSampleCount * samplesIntervalMillis),
                samplesIntervalMillis
            );
            ingestSamples(moreSamples, indexName);
        }

        logger.info("--> add replicas again");
        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("index.number_of_replicas", 1))
        );
        ensureGreen(indexName);

        // Validate that replica has recovered all data (initial + additional samples)
        int totalSampleCount = initialSampleCount + additionalSampleCount;
        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, totalSampleCount);

        logger.info("--> repeated recovery test completed successfully");
    }

    /**
     * Tests that peer recovery trims local translog after replaying operations.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testPeerRecoveryTrimsLocalTranslog()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with replica on data nodes</li>
     *   <li>Start concurrent indexing threads</li>
     *   <li>Inject failures to simulate network issues during replication</li>
     *   <li>Force primary failover by preventing shard failure notifications</li>
     *   <li>Restart old primary node</li>
     *   <li>Verify it recovers from new primary</li>
     * </ol>
     *
     */
    public void testPeerRecoveryCompletesAfterPrimaryDemotion() throws Exception {
        internalCluster().startNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);

        String indexName = "test-index";
        // Start with default TSDB settings and add routing allocation
        java.util.HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put("index.routing.allocation.include._name", String.join(",", dataNodes));

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 1, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(indexName);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode nodeWithOldPrimary = clusterState.nodes()
            .get(clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId());
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nodeWithOldPrimary.getName()
        );
        CountDownLatch readyToRestartNode = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();

        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals("indices:data/write/bulk[s][r]") && randomInt(100) < 5) {
                throw new NodeClosedException(nodeWithOldPrimary);
            }
            // prevent the primary from marking the replica as stale so the replica can get promoted.
            if (action.equals("internal:cluster/shard/failure")) {
                stopped.set(true);
                readyToRestartNode.countDown();
                throw new NodeClosedException(nodeWithOldPrimary);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        Thread[] indexers = new Thread[randomIntBetween(1, 8)];
        for (int i = 0; i < indexers.length; i++) {
            indexers[i] = new Thread(() -> {
                while (stopped.get() == false) {
                    try {
                        long timestamp = System.currentTimeMillis();
                        TimeSeriesSample sample = new TimeSeriesSample(
                            java.time.Instant.ofEpochMilli(timestamp),
                            randomNonNegativeLong() * 1.0,
                            java.util.Map.of("metric", "cpu_usage", "instance", "pod" + randomIntBetween(1, 10), "env", "prod")
                        );
                        ingestSamples(List.of(sample), indexName);
                    } catch (Exception ignored) {}
                }
            });
        }

        for (Thread indexer : indexers) {
            indexer.start();
        }
        readyToRestartNode.await();
        transportService.clearAllRules();
        internalCluster().restartNode(nodeWithOldPrimary.getName(), new InternalTestCluster.RestartCallback());
        for (Thread indexer : indexers) {
            indexer.join();
        }
        ensureGreen(indexName);
    }
}
