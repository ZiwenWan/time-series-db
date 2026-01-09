/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.indices.recovery.PeerRecoverySourceService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for TSDB recovery resilience scenarios.
 * Tests recovery behavior under failure conditions like network disconnects and transient errors.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TSDBRecoveryResilienceIT extends TSDBRecoveryITBase {

    /**
     * Tests that transient errors during recovery are retried.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testTransientErrorsDuringRecoveryAreRetried()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index on blue node with samples</li>
     *   <li>Inject random transient errors during recovery (NodeClosedException)</li>
     *   <li>Add replica on red node to trigger recovery</li>
     *   <li>Verify recovery succeeds despite transient errors</li>
     *   <li>Validate TSDB data on both primary and replica</li>
     * </ol>
     *
     * <p>Tests network failures, rejections, and circuit breaker exceptions during various
     * recovery phases (FILES_INFO, FILE_CHUNK, CLEAN_FILES, PREPARE_TRANSLOG, TRANSLOG_OPS, FINALIZE).
     */
    public void testTransientErrorsDuringRecoveryAreRetried() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "500ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "10s")
            .build();

        // start a cluster-manager node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        // Start with default TSDB settings and add routing to blue node
        java.util.HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue");

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 0, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);

        int numSamples = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, indexName);
        client().admin().indices().prepareFlush(indexName).setForce(true).get();
        ensureSearchable(indexName);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);

        String[] recoveryActions = new String[] {
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.FINALIZE };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        logger.info("--> will temporarily interrupt recovery action between blue & red on [{}]", recoveryActionToBlock);

        MockTransportService blueTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );

        final AtomicBoolean recoveryStarted = new AtomicBoolean(false);
        final AtomicBoolean finalizeReceived = new AtomicBoolean(false);
        final AtomicInteger blocksRemaining = new AtomicInteger(randomIntBetween(1, 3));

        redTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
            recoveryStarted.set(true);
            if (recoveryActionToBlock.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalizeReceived.set(true);
            }
            if (blocksRemaining.getAndUpdate(i -> i == 0 ? 0 : i - 1) != 0) {
                String reason = randomFrom("rejected", "circuit", "network");
                if (reason.equals("rejected")) {
                    logger.info("--> preventing {} response by throwing exception", recoveryActionToBlock);
                    throw new OpenSearchRejectedExecutionException();
                } else if (reason.equals("circuit")) {
                    logger.info("--> preventing {} response by throwing exception", recoveryActionToBlock);
                    throw new CircuitBreakingException("Broken", CircuitBreaker.Durability.PERMANENT);
                } else if (reason.equals("network")) {
                    logger.info("--> preventing {} response by breaking connection", recoveryActionToBlock);
                    blueTransportService.disconnectFromNode(redTransportService.getLocalDiscoNode());
                    if (randomBoolean()) {
                        redTransportService.disconnectFromNode(blueTransportService.getLocalDiscoNode());
                    }
                }
            }
            handler.messageReceived(request, channel, task);
        });

        try {
            logger.info("--> starting recovery from blue to red");
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    )
            );

            ensureGreen();
            validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);
        } finally {
            blueTransportService.clearAllRules();
            redTransportService.clearAllRules();
        }
    }

    /**
     * Tests recovery resilience to network disconnections during different recovery phases.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testDisconnectsWhileRecovering()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index on blue node with samples</li>
     *   <li>Randomly select a recovery action to disrupt (START_RECOVERY, FILES_INFO, etc.)</li>
     *   <li>Either drop requests or break connection during that action</li>
     *   <li>Add replica on red node to trigger recovery</li>
     *   <li>Verify recovery eventually succeeds despite disconnections</li>
     *   <li>Validate TSDB data integrity</li>
     * </ol>
     */
    public void testDisconnectsWhileRecovering() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "1s")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "1s")
            .build();

        // start a cluster-manager node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        // Start with default TSDB settings and add routing to blue node
        java.util.HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue");

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 0, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);

        int numSamples = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, indexName);
        ensureSearchable(indexName);
        client().admin().indices().prepareFlush(indexName).setForce(true).get();

        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);

        String[] recoveryActions = new String[] {
            PeerRecoverySourceService.Actions.START_RECOVERY,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.FINALIZE };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);

        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );
        TransportService redTransportService = internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = internalCluster().getInstance(TransportService.class, blueNodeName);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            // Fail on the sending side
            blueMockTransportService.addSendBehavior(
                redTransportService,
                new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, requestFailed)
            );
            redMockTransportService.addSendBehavior(
                blueTransportService,
                new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, requestFailed)
            );
        } else {
            // Fail on the receiving side.
            blueMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                redMockTransportService.disconnectFromNode(blueMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel, task);
            });
            redMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                blueMockTransportService.disconnectFromNode(redMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel, task);
            });
        }

        logger.info("--> starting recovery from blue to red");
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
            .get();

        requestFailed.await();

        logger.info("--> clearing rules to allow recovery to proceed");
        blueMockTransportService.clearAllRules();
        redMockTransportService.clearAllRules();

        ensureGreen();
        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamples);
    }

    private class RecoveryActionBlocker implements StubbableTransport.SendRequestBehavior {
        private final boolean dropRequests;
        private final String recoveryActionToBlock;
        private final CountDownLatch requestBlocked;

        RecoveryActionBlocker(boolean dropRequests, String recoveryActionToBlock, CountDownLatch requestBlocked) {
            this.dropRequests = dropRequests;
            this.recoveryActionToBlock = recoveryActionToBlock;
            this.requestBlocked = requestBlocked;
        }

        @Override
        public void sendRequest(
            Transport.Connection connection,
            long requestId,
            String action,
            TransportRequest request,
            TransportRequestOptions options
        ) throws IOException {
            if (recoveryActionToBlock.equals(action) || requestBlocked.getCount() == 0) {
                requestBlocked.countDown();
                if (dropRequests) {
                    logger.info("--> preventing {} request by dropping request", action);
                    return;
                } else {
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        }
    }
}
