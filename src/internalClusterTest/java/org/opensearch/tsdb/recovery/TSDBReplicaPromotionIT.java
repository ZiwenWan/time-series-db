/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

/**
 * Integration tests for TSDB replica-to-primary promotion scenarios.
 * Tests replica promotion when primary shard fails or node goes down.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class TSDBReplicaPromotionIT extends TSDBRecoveryITBase {

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    /**
     * Tests replica promotion to primary after primary node failure.
     *
     * <p>Adopted from: {@code ReplicaToPrimaryPromotionIT.testPromoteReplicaToPrimary()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with 1 primary + 1 replica on 2 nodes</li>
     *   <li>Index time series samples</li>
     *   <li>Validate samples exist on both primary and replica</li>
     *   <li>Stop node hosting primary shard</li>
     *   <li>Replica is promoted to primary</li>
     *   <li>Validate data consistency after promotion</li>
     * </ol>
     */
    public void testPromoteReplicaToPrimary() throws Exception {
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        logger.info("--> creating TSDB index with 1 shard, 1 replica");
        IndexConfig indexConfig = createDefaultIndexConfig(indexName, 1, 1);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(indexName);

        // Index samples
        int numSamples = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, indexName);
        refreshAndWaitForReplication(indexName);
        client().admin().indices().prepareFlush(indexName).setForce(true).get();

        logger.info("--> indexed {} samples", numSamples);

        // Verify samples count before promotion
        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);

        // Identify primary and replica nodes
        ClusterState state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        final int numShards = state.metadata().index(indexName).getNumberOfShards();
        final ShardRouting primaryShard = state.routingTable().index(indexName).shard(randomIntBetween(0, numShards - 1)).primaryShard();
        final DiscoveryNode primaryNode = state.nodes().resolveNode(primaryShard.currentNodeId());

        // Find replica node
        final IndexShardRoutingTable shardRoutingTable = state.routingTable().index(indexName).shard(primaryShard.id());
        String replicaNodeName = null;
        for (ShardRouting routing : shardRoutingTable) {
            if (!routing.primary() && routing.assignedToNode()) {
                replicaNodeName = state.nodes().get(routing.currentNodeId()).getName();
                break;
            }
        }
        assertNotNull("Replica node should be found", replicaNodeName);

        logger.info("--> stopping primary node {} to trigger replica promotion on node {}", primaryNode.getName(), replicaNodeName);

        // Stop primary node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode.getName()));
        ensureYellowAndNoInitializingShards(indexName);

        // Verify replica was promoted
        state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        for (IndexShardRoutingTable shardRouting : state.routingTable().index(indexName)) {
            for (ShardRouting routing : shardRouting.activeShards()) {
                assertThat(routing + " should be promoted as primary", routing.primary(), is(true));
            }
        }

        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);
    }
}
