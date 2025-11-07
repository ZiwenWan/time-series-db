/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.store.Directory;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.plugin.store.subdirectory.SubdirectoryAwareStore;
import org.opensearch.plugins.IndexStorePlugin;

/**
 * TSDB-specific store implementation that extends SubdirectoryAwareStore.
 */
public class TSDBStore extends SubdirectoryAwareStore {

    /**
     * Constructs a new TSDBStore instance.
     *
     * @param shardId       The shard identifier
     * @param indexSettings The index settings
     * @param directory     The Lucene directory for storage
     * @param shardLock     The shard lock for concurrency control
     * @param onClose       Callback executed when the store is closed
     * @param shardPath     The file system path for the shard
     */
    public TSDBStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        Store.OnClose onClose,
        ShardPath shardPath
    ) {
        super(shardId, indexSettings, directory, shardLock, onClose, shardPath);
    }

    /**
     * Constructs a new TSDBStore instance.
     *
     * @param shardId       The shard identifier
     * @param indexSettings The index settings
     * @param directory     The Lucene directory for storage
     * @param shardLock     The shard lock for concurrency control
     * @param onClose       Callback executed when the store is closed
     * @param shardPath     The file system path for the shard
     * @param directoryFactory the directory factory
     */
    public TSDBStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        Store.OnClose onClose,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory directoryFactory
    ) {
        super(shardId, indexSettings, directory, shardLock, onClose, shardPath, directoryFactory);
    }
}
