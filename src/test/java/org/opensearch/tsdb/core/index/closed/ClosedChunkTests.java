/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.chunk.XORIterator;

public class ClosedChunkTests extends OpenSearchTestCase {

    public void testGetters() {
        XORChunk xorChunk = new XORChunk();

        ClosedChunk chunk = new ClosedChunk(1000L, 2000L, xorChunk.bytes(), Encoding.XOR);

        assertEquals(1000L, chunk.getMinTimestamp());
        assertEquals(2000L, chunk.getMaxTimestamp());
        assertNotNull(chunk.getChunkIterator());
        assertTrue(chunk.getChunkIterator() instanceof XORIterator);
    }

    public void testIterateConstruction() {
        XORChunk xorChunk = new XORChunk();
        ChunkAppender appender = xorChunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);

        ClosedChunk chunk = new ClosedChunk(1000L, 2000L, xorChunk.bytes(), Encoding.XOR);
        ChunkIterator iterator = chunk.getChunkIterator();

        assertNotNull(iterator);

        assertEquals(ChunkIterator.ValueType.FLOAT, iterator.next());
        ChunkIterator.TimestampValue tv = iterator.at();
        assertEquals(1000L, tv.timestamp());
        assertEquals(10.0, tv.value(), 0.001);

        assertEquals(ChunkIterator.ValueType.FLOAT, iterator.next());
        tv = iterator.at();
        assertEquals(2000L, tv.timestamp());
        assertEquals(20.0, tv.value(), 0.001);
    }
}
