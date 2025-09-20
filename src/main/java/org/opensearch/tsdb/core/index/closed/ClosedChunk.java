/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORIterator;

/**
 * Implementation of a completed (closed) head chunk.
 *
 * ClosedChunk provides read-only access to compressed chunk data, as well as access to chunk metadata without requiring decompression.
 */
public class ClosedChunk {

    private final long minTimestamp;
    private final long maxTimestamp;
    private final ChunkIterator chunkIterator;

    /**
     * Constructs a ClosedChunk with the given parameters.
     * @param minTimestamp the minimum timestamp in the chunk
     * @param maxTimestamp the maximum timestamp in the chunk
     * @param bytes the compressed chunk data
     * @param encoding the encoding format used for the chunk data
     */
    public ClosedChunk(long minTimestamp, long maxTimestamp, byte[] bytes, Encoding encoding) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.chunkIterator = switch (encoding) {
            case XOR -> new XORIterator(bytes);
        };
    }

    /**
     * Returns the minimum timestamp of the chunk.
     * @return the minimum timestamp
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Returns the maximum timestamp of the chunk.
     * @return the maximum timestamp
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Returns an iterator over the samples in the chunk.
     * @return the ChunkIterator
     */
    public ChunkIterator getChunkIterator() {
        return chunkIterator;
    }
}
