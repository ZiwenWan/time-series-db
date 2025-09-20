/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.test.OpenSearchTestCase;

public class ChunkOptionsTests extends OpenSearchTestCase {

    public void testValidChunkOptions() {
        ChunkOptions options = new ChunkOptions(1000L, 5);
        assertEquals(1000L, options.chunkRange());
        assertEquals(5, options.samplesPerChunk());
    }

    public void testExceptionMessage() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ChunkOptions(1000L, 3));
        assertEquals("samplesPerChunk must be >= 4, got: 3", exception.getMessage());
    }
}
