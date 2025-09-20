/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

/**
 * Configuration options for chunk management.
 *
 * @param chunkRange the time range covered by each chunk
 * @param samplesPerChunk the target number of samples per chunk
 */
public record ChunkOptions(long chunkRange, int samplesPerChunk) {

    /**
     * ChunkOptions constructor with validations.
     */
    public ChunkOptions {
        // Chunk time boundary prediction requires a minimum of 4 samples per chunk. It aims to distribute chunks evenly across
        // based on the first 25% of samples of each chunk, reducing the occurrence of abnormally small/large chunks. Rather than
        // adding branching to handle < 4 samples per chunk, we codify the constraint as using tiny chunks is not intended usage.
        if (samplesPerChunk < 4) {
            throw new IllegalArgumentException("samplesPerChunk must be >= 4, got: " + samplesPerChunk);
        }
    }
}
