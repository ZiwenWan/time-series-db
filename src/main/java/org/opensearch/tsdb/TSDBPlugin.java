/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.TSDBEngineFactory;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.rest.RestM3QLAction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Plugin for time-series database (TSDB) engine.
 *
 * <p>This plugin provides time series database functionality including:
 * <ul>
 *   <li>TSDB storage engine</li>
 *   <li>Time series aggregations</li>
 *   <li>M3QL query support</li>
 *   <li>Custom store implementation</li>
 * </ul>
 */
public class TSDBPlugin extends Plugin implements SearchPlugin, EnginePlugin, ActionPlugin, IndexStorePlugin {

    // Search plugin constants
    private static final String TIME_SERIES_NAMED_WRITEABLE_NAME = "time_series";

    // Store plugin constants
    private static final String TSDB_STORE_FACTORY_NAME = "tsdb_store";

    // Management thread pool name to run tasks like retention and compactions.
    public static final String MGMT_THREAD_POOL_NAME = "mgmt";

    /**
     * This setting identifies if the tsdb engine is enabled for the index.
     */
    public static final Setting<Boolean> TSDB_ENGINE_ENABLED = Setting.boolSetting(
        "index.tsdb_engine.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> TSDB_ENGINE_RETENTION_TIME = Setting.timeSetting(
        "index.tsdb_engine.retention.time",
        TimeValue.MINUS_ONE,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> TSDB_ENGINE_RETENTION_FREQUENCY = Setting.timeSetting(
        "index.tsdb_engine.retention.frequency",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<String> TSDB_ENGINE_COMPACTION_TYPE = Setting.simpleString(
        "index.tsdb_engine.compaction.type",
        "SizeTieredCompaction",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> TSDB_ENGINE_COMPACTION_FREQUENCY = Setting.timeSetting(
        "index.tsdb_engine.compaction.frequency",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the target number of samples to store in a single chunk.
     * <p>
     * TODO: consume this setting in the TSDB engine implementation to allow changes to be picked up quickly.
     */
    public static final Setting<Integer> TSDB_ENGINE_SAMPLES_PER_CHUNK = Setting.intSetting(
        "index.tsdb_engine.chunk.samples_per_chunk",
        120,
        4,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the chunk expiry duration. Non-full chunks will be closed after this grace period if no new samples come in. May be
     * used to prevent sparse samples from each creating their own chunk.
     * <p>
     * TODO: consume this setting in the TSDB engine implementation to allow changes to be picked up quickly.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_CHUNK_EXPIRY = Setting.positiveTimeSetting(
        "index.tsdb_engine.chunk.expiry",
        TimeValue.timeValueMinutes(30),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the block duration for closed chunk indexes.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_BLOCK_DURATION = Setting.positiveTimeSetting(
        "index.tsdb_engine.block.duration",
        TimeValue.timeValueHours(2),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the default time unit used for sample storage.
     */
    public static final Setting<String> TSDB_ENGINE_TIME_UNIT = Setting.simpleString(
        "index.tsdb_engine.time_unit",
        Constants.Time.DEFAULT_TIME_UNIT.toString(),
        value -> {
            try {
                TimeUnit unit = TimeUnit.valueOf(value);
                if (unit != TimeUnit.MILLISECONDS) {
                    throw new IllegalArgumentException(); // TODO: support additional time units when properly handled

                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid time unit: " + value + ". Only MILLISECONDS currently supported");
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default constructor
     */
    public TSDBPlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TSDB_ENGINE_ENABLED,
            TSDB_ENGINE_RETENTION_TIME,
            TSDB_ENGINE_RETENTION_FREQUENCY,
            TSDB_ENGINE_COMPACTION_TYPE,
            TSDB_ENGINE_COMPACTION_FREQUENCY,
            TSDB_ENGINE_SAMPLES_PER_CHUNK,
            TSDB_ENGINE_CHUNK_EXPIRY,
            TSDB_ENGINE_BLOCK_DURATION,
            TSDB_ENGINE_TIME_UNIT
        );
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            new AggregationSpec(
                TimeSeriesUnfoldAggregationBuilder.NAME,
                TimeSeriesUnfoldAggregationBuilder::new,
                TimeSeriesUnfoldAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new).setAggregatorRegistrar(TimeSeriesUnfoldAggregationBuilder::registerAggregators)
        );
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return List.of(
            // Register TimeSeriesCoordinatorAggregation
            new PipelineAggregationSpec(
                TimeSeriesCoordinatorAggregationBuilder.NAME,
                TimeSeriesCoordinatorAggregationBuilder::new,
                TimeSeriesCoordinatorAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new)
        );
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (TSDB_ENGINE_ENABLED.get(indexSettings.getSettings())) {
            return Optional.of(new TSDBEngineFactory());
        }
        return Optional.empty();
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestM3QLAction());
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(InternalAggregation.class, TIME_SERIES_NAMED_WRITEABLE_NAME, InternalTimeSeries::new)
        );
    }

    @Override
    public Map<String, StoreFactory> getStoreFactories() {
        Map<String, StoreFactory> map = new HashMap<>();
        map.put(TSDB_STORE_FACTORY_NAME, new TSDBStoreFactory());
        return Collections.unmodifiableMap(map);
    }

    /**
     * Factory for creating TSDB store instances.
     */
    static class TSDBStoreFactory implements StoreFactory {
        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath
        ) throws IOException {
            return new TSDBStore(shardId, indexSettings, directory, shardLock, onClose, shardPath);
        }

        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath,
            DirectoryFactory directoryFactory
        ) throws IOException {
            return new TSDBStore(shardId, indexSettings, directory, shardLock, onClose, shardPath, directoryFactory);
        }
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        var executorBuilder = new FixedExecutorBuilder(
            settings,
            MGMT_THREAD_POOL_NAME,
            1,
            1,
            "index.tsdb_engine.thread_pool." + MGMT_THREAD_POOL_NAME
        );
        return List.of(executorBuilder);
    }
}
