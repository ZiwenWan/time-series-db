/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.opensearch.tsdb.framework.models.IndexConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.junit.Assert.assertFalse;

/**
 * Integration tests for TSDB recovery during cluster state changes.
 * Tests recovery continues through cluster manager failover.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TSDBRecoveryClusterStateIT extends TSDBRecoveryITBase {

    /**
     * Tests that ongoing recovery continues through cluster manager failover (ASF disabled).
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testOngoingRecoveryAndClusterManagerFailOverForASFDisabled()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Start 2 cluster manager nodes with ASF disabled</li>
     *   <li>Start data node with TSDB index containing primary shard</li>
     *   <li>Block recovery on CLEAN_FILES action</li>
     *   <li>Start second data node to trigger replica recovery</li>
     *   <li>Fail over cluster manager while recovery is in progress</li>
     *   <li>Unblock recovery and verify it completes successfully</li>
     * </ol>
     */
    public void testOngoingRecoveryAndClusterManagerFailOverForASFDisabled() throws Exception {
        String indexName = "test";
        // ASF Disabled
        internalCluster().startNodes(
            2,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), false).build()
        );

        String nodeWithPrimary = internalCluster().startDataOnlyNode();

        java.util.HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put("index.routing.allocation.include._name", nodeWithPrimary);

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 0, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);

        MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        Semaphore blockRecovery = new Semaphore(1);

        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action) && blockRecovery.tryAcquire()) {
                phase1ReadyBlocked.countDown();
                try {
                    allowToCompletePhase1Latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try {
            String nodeWithReplica = internalCluster().startDataOnlyNode();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put("index.routing.allocation.include._name", nodeWithPrimary + "," + nodeWithReplica)
                    )
            );
            phase1ReadyBlocked.await();
            internalCluster().restartNode(
                clusterService().state().nodes().getClusterManagerNode().getName(),
                new InternalTestCluster.RestartCallback()
            );
            internalCluster().ensureAtLeastNumDataNodes(3);
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                            .putNull("index.routing.allocation.include._name")
                    )
            );
            assertFalse(client().admin().cluster().prepareHealth(indexName).setWaitForActiveShards(2).get().isTimedOut());
        } finally {
            allowToCompletePhase1Latch.countDown();
        }
        ensureGreen(indexName);
    }

    /**
     * Tests that ongoing recovery continues through cluster manager failover (ASF enabled).
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testOngoingRecoveryAndClusterManagerFailOver()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Start 2 cluster manager nodes with ASF enabled (default)</li>
     *   <li>Start data node with TSDB index containing primary shard</li>
     *   <li>Block recovery on CLEAN_FILES action</li>
     *   <li>Start second data node to trigger replica recovery</li>
     *   <li>Fail over cluster manager while recovery is in progress</li>
     *   <li>Unblock recovery and verify it completes successfully</li>
     * </ol>
     */
    public void testOngoingRecoveryAndClusterManagerFailOver() throws Exception {
        String indexName = "test";
        internalCluster().startNodes(2);
        String nodeWithPrimary = internalCluster().startDataOnlyNode();

        java.util.HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put("index.routing.allocation.include._name", nodeWithPrimary);

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 0, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);

        MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        Semaphore blockRecovery = new Semaphore(1);

        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action) && blockRecovery.tryAcquire()) {
                phase1ReadyBlocked.countDown();
                try {
                    allowToCompletePhase1Latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try {
            String nodeWithReplica = internalCluster().startDataOnlyNode();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put("index.routing.allocation.include._name", nodeWithPrimary + "," + nodeWithReplica)
                    )
            );
            phase1ReadyBlocked.await();
            internalCluster().restartNode(
                clusterService().state().nodes().getClusterManagerNode().getName(),
                new InternalTestCluster.RestartCallback()
            );
            internalCluster().ensureAtLeastNumDataNodes(3);
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                            .putNull("index.routing.allocation.include._name")
                    )
            );

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue(
                state.routingTable().index(indexName).shardsWithState(ShardRoutingState.UNASSIGNED).size() == 1
                    && state.routingTable().index(indexName).shardsWithState(ShardRoutingState.INITIALIZING).size() == 1
            );

            // Shard assignment may be stuck because recovery is blocked at CLEAN_FILES stage
            // Wait with extended timeout
            ensureGreen(TimeValue.timeValueSeconds(62), indexName);
        } finally {
            allowToCompletePhase1Latch.countDown();
        }
    }
}
