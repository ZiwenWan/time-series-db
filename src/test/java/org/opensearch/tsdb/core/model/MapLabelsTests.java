/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.tsdb.core.model;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MapLabelsTests extends OpenSearchTestCase {

    public void testBasicFunctionality() {
        MapLabels labels = MapLabels.fromStrings("k1", "v1", "k2", "v2");

        assertEquals("v1", labels.get("k1"));
        assertEquals("v2", labels.get("k2"));
        assertEquals("", labels.get("nonexistent"));
        assertTrue(labels.has("k1"));
        assertFalse(labels.has("nonexistent"));
        assertFalse(labels.isEmpty());
    }

    public void testInvalidInput() {
        expectThrows(IllegalArgumentException.class, () -> MapLabels.fromStrings("k1", "v1", "k2"));
    }

    public void testConstructorFromMap() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        MapLabels labels = new MapLabels(map);

        assertEquals("value1", labels.get("key1"));
        assertTrue(labels.has("key1"));
    }

    public void testSerialization() throws IOException {
        MapLabels original = MapLabels.fromStrings("k1", "v1", "k2", "v2");

        byte[] bytes = new byte[16 * 1024];
        int pos = original.bytes(bytes);
        MapLabels deserialized = MapLabels.fromSerializedBytes(Arrays.copyOfRange(bytes, 0, pos));

        assertEquals(original, deserialized);
    }

    public void testEmptyLabels() {
        MapLabels empty = MapLabels.emptyLabels();
        assertTrue(empty.isEmpty());
        assertEquals("", empty.toKeyValueString());
    }

    public void testStableHash() {
        MapLabels labels1 = MapLabels.fromStrings("k1", "v1", "k2", "v2");
        MapLabels labels2 = MapLabels.fromStrings("k2", "v2", "k1", "v1");

        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1.hashCode(), labels2.hashCode());
    }
}
