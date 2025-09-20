/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.XORChunk;

public class MemChunkTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        MemChunk chunk = new MemChunk(1L, 1000L, 2000L, null);

        assertEquals(1L, chunk.getMinSeqNo());
        assertEquals(1000L, chunk.getMinTimestamp());
        assertEquals(2000L, chunk.getMaxTimestamp());
        assertNull(chunk.getPrev());
        assertNull(chunk.getNext());
        assertNull(chunk.getChunk());
    }

    public void testLinkedListOperations() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null);
        MemChunk second = new MemChunk(2L, 2000L, 3000L, first);

        assertNull(first.getPrev());
        assertEquals(first, second.getPrev());
        assertEquals(second, first.getNext());
        assertNull(second.getNext());
    }

    public void testLength() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null);
        assertEquals(1, first.len());

        MemChunk second = new MemChunk(2L, 2000L, 3000L, first);
        assertEquals(2, second.len());

        MemChunk third = new MemChunk(3L, 3000L, 4000L, second);
        assertEquals(3, third.len());
    }

    public void testOldest() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null);
        MemChunk second = new MemChunk(2L, 2000L, 3000L, first);
        MemChunk third = new MemChunk(3L, 3000L, 4000L, second);

        assertEquals(first, third.oldest());
        assertEquals(first, second.oldest());
        assertEquals(first, first.oldest());
    }

    public void testAtOffset() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null);
        MemChunk second = new MemChunk(2L, 2000L, 3000L, first);
        MemChunk third = new MemChunk(3L, 3000L, 4000L, second);

        assertEquals(third, third.atOffset(0));
        assertEquals(second, third.atOffset(1));
        assertEquals(first, third.atOffset(2));
        assertNull(third.atOffset(10));
        assertNull(third.atOffset(-1));
    }

    public void testSettersAndGetters() {
        MemChunk chunk = new MemChunk(1L, 1000L, 2000L, null);
        XORChunk xorChunk = new XORChunk();

        chunk.setChunk(xorChunk);
        assertEquals(xorChunk, chunk.getChunk());

        chunk.setMinTimestamp(500L);
        assertEquals(500L, chunk.getMinTimestamp());

        chunk.setMaxTimestamp(2500L);
        assertEquals(2500L, chunk.getMaxTimestamp());

        MemChunk other = new MemChunk(2L, 2000L, 3000L, null);
        chunk.setNext(other);
        assertEquals(other, chunk.getNext());

        chunk.setPrev(other);
        assertEquals(other, chunk.getPrev());
    }
}
