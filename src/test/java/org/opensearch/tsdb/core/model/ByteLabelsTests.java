/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class ByteLabelsTests extends OpenSearchTestCase {

    public void testBasicFunctionality() {
        ByteLabels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        assertEquals("v1", labels.get("k1"));
        assertEquals("v2", labels.get("k2"));
        assertEquals("", labels.get("nonexistent"));
        assertEquals("", labels.get(null));
        assertEquals("", labels.get(""));
        assertTrue(labels.has("k1"));
        assertFalse(labels.has("nonexistent"));
        assertFalse(labels.has(null));
        assertFalse(labels.has(""));
        assertFalse(labels.isEmpty());

        // Test toKeyValueString
        String kvString = labels.toKeyValueString();
        assertTrue("Should contain k1:v1", kvString.contains("k1:v1"));
        assertTrue("Should contain k2:v2", kvString.contains("k2:v2"));

        // Test toString
        assertEquals("toString should match toKeyValueString", kvString, labels.toString());
    }

    public void testInvalidInput() {
        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromStrings("k1", "v1", "k2"));
    }

    public void testEmptyLabels() {
        ByteLabels empty = ByteLabels.emptyLabels();
        assertTrue(empty.isEmpty());
        assertEquals("", empty.toKeyValueString());
        assertEquals("", empty.toString());
        assertEquals("", empty.get("anything"));
        assertFalse(empty.has("anything"));

        ByteLabels emptyFromMap = ByteLabels.fromMap(Map.of());
        assertTrue(emptyFromMap.isEmpty());
        assertEquals(empty, emptyFromMap);
    }

    public void testLabelSorting() {
        ByteLabels labels1 = ByteLabels.fromStrings("zebra", "z", "apple", "a");
        ByteLabels labels2 = ByteLabels.fromStrings("apple", "a", "zebra", "z");

        assertEquals(labels1.toMapView(), labels2.toMapView());
        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1, labels2);
    }

    public void testStableHash() {
        ByteLabels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        ByteLabels labels2 = ByteLabels.fromStrings("k2", "v2", "k1", "v1");

        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1.hashCode(), labels2.hashCode());
    }

    public void testLongStringEncoding() {
        // Create a string longer than 254 bytes to test extended encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);

        ByteLabels labels = ByteLabels.fromStrings(longKey, longValue, "short", "val");

        assertEquals(longValue, labels.get(longKey));
        assertEquals("val", labels.get("short"));
        assertTrue(labels.has(longKey));

        // Verify it works with fromMap too
        ByteLabels labels2 = ByteLabels.fromMap(Map.of(longKey, longValue, "short", "val"));
        assertEquals(labels, labels2);
    }

    public void testEqualsAndHashCode() {
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2");
        ByteLabels labels2 = ByteLabels.fromStrings("b", "2", "a", "1"); // Different order
        ByteLabels labels3 = ByteLabels.fromStrings("a", "1", "b", "3"); // Different value

        // Test equals
        assertEquals("Same labels should be equal", labels1, labels2);
        assertNotEquals("Different labels should not be equal", labels1, labels3);

        // Test hashCode consistency
        assertEquals("Equal objects should have same hashCode", labels1.hashCode(), labels2.hashCode());
    }

    public void testToKeyValueBytesRefs() {
        ByteLabels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        BytesRef[] result = labels.toKeyValueBytesRefs();

        assertEquals("Should have 2 label pairs", 2, result.length);

        // Verify the content of each BytesRef
        assertEquals("k1" + LabelConstants.LABEL_DELIMITER + "v1", result[0].utf8ToString());
        assertEquals("k2" + LabelConstants.LABEL_DELIMITER + "v2", result[1].utf8ToString());

        // Verify they are in sorted order (k1 before k2)
        assertEquals("First label should be k1 v1", "k1" + LabelConstants.LABEL_DELIMITER + "v1", result[0].utf8ToString());
        assertEquals("Second label should be k2 v2", "k2" + LabelConstants.LABEL_DELIMITER + "v2", result[1].utf8ToString());
    }

    public void testToKeyValueBytesRefsEmpty() {
        ByteLabels empty = ByteLabels.emptyLabels();
        BytesRef[] result = empty.toKeyValueBytesRefs();

        assertEquals("Empty labels should return empty array", 0, result.length);
    }

    public void testToKeyValueBytesRefsWithLongStrings() {
        // >250 is chosen to test decoding with ByteLabels' var length encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);

        ByteLabels labels = ByteLabels.fromStrings(longKey, longValue, "short", "val");
        BytesRef[] result = labels.toKeyValueBytesRefs();

        assertEquals("Should have 2 label pairs", 2, result.length);

        // Find which result corresponds to which label (they're sorted)
        BytesRef shortResult = null, longResult = null;
        for (BytesRef ref : result) {
            if (ref.utf8ToString().startsWith("short")) {
                shortResult = ref;
            } else if (ref.utf8ToString().startsWith("very_long_key_")) {
                longResult = ref;
            }
        }

        assertNotNull("Should find short label", shortResult);
        assertNotNull("Should find long label", longResult);
        assertEquals("short" + LabelConstants.LABEL_DELIMITER + "val", shortResult.utf8ToString());
        assertEquals(longKey + LabelConstants.LABEL_DELIMITER + longValue, longResult.utf8ToString());
    }

    public void testToKeyValueBytesRefsConsistencyWithToKeyValueString() {
        ByteLabels labels = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");

        BytesRef[] bytesRefs = labels.toKeyValueBytesRefs();
        String keyValueString = labels.toKeyValueString();

        // Convert BytesRef array to space-separated string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytesRefs.length; i++) {
            if (i > 0) sb.append(" ");
            sb.append(bytesRefs[i].utf8ToString());
        }

        assertEquals("BytesRef array should match toKeyValueString", keyValueString, sb.toString());
    }

    public void testFromSortedKeyValuePairs() {
        List<String> keyValuePairs = List.of(
            "a" + LabelConstants.LABEL_DELIMITER + "1",
            "b" + LabelConstants.LABEL_DELIMITER + "2",
            "c" + LabelConstants.LABEL_DELIMITER + "3"
        );
        ByteLabels labels = ByteLabels.fromSortedKeyValuePairs(keyValuePairs);

        assertEquals("1", labels.get("a"));
        assertEquals("2", labels.get("b"));
        assertEquals("3", labels.get("c"));
        assertEquals("", labels.get("nonexistent"));
        assertTrue(labels.has("a"));
        assertTrue(labels.has("b"));
        assertTrue(labels.has("c"));
        assertFalse(labels.has("nonexistent"));
        assertFalse(labels.isEmpty());

        String kvString = labels.toKeyValueString();
        assertEquals(
            "a" + LabelConstants.LABEL_DELIMITER + "1 b" + LabelConstants.LABEL_DELIMITER + "2 c" + LabelConstants.LABEL_DELIMITER + "3",
            kvString
        );
    }

    public void testFromSortedKeyValuePairsEmpty() {
        ByteLabels empty1 = ByteLabels.fromSortedKeyValuePairs(null);
        ByteLabels empty2 = ByteLabels.fromSortedKeyValuePairs(List.of());

        assertTrue(empty1.isEmpty());
        assertTrue(empty2.isEmpty());
        assertEquals(ByteLabels.emptyLabels(), empty1);
        assertEquals(ByteLabels.emptyLabels(), empty2);
    }

    public void testFromSortedKeyValuePairsInvalidInput() {
        // Missing delimiter
        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromSortedKeyValuePairs(List.of("key_without_delim")));

        // Delimiter at start
        expectThrows(
            IllegalArgumentException.class,
            () -> ByteLabels.fromSortedKeyValuePairs(List.of(LabelConstants.LABEL_DELIMITER + "value"))
        );

        // Delimiter at end
        expectThrows(
            IllegalArgumentException.class,
            () -> ByteLabels.fromSortedKeyValuePairs(List.of("key" + LabelConstants.LABEL_DELIMITER))
        );

        // Only delimiter
        expectThrows(
            IllegalArgumentException.class,
            () -> ByteLabels.fromSortedKeyValuePairs(List.of(String.valueOf(LabelConstants.LABEL_DELIMITER)))
        );
    }

    public void testFromSortedKeyValuePairsConsistencyWithFromStrings() {
        List<String> keyValuePairs = List.of(
            "a" + LabelConstants.LABEL_DELIMITER + "1",
            "b" + LabelConstants.LABEL_DELIMITER + "2",
            "z" + LabelConstants.LABEL_DELIMITER + "9"
        );
        ByteLabels labelsFromKV = ByteLabels.fromSortedKeyValuePairs(keyValuePairs);
        ByteLabels labelsFromStrings = ByteLabels.fromStrings("a", "1", "b", "2", "z", "9");

        assertEquals(labelsFromKV, labelsFromStrings);
        assertEquals(labelsFromKV.stableHash(), labelsFromStrings.stableHash());
        assertEquals(labelsFromKV.toKeyValueString(), labelsFromStrings.toKeyValueString());
    }

    public void testFromSortedKeyValuePairsLongValues() {
        // >250 is chosen to test decoding with ByteLabels' var length encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);
        List<String> keyValuePairs = List.of(
            "a" + LabelConstants.LABEL_DELIMITER + "1",
            longKey + LabelConstants.LABEL_DELIMITER + longValue
        );

        ByteLabels labels = ByteLabels.fromSortedKeyValuePairs(keyValuePairs);

        assertEquals("1", labels.get("a"));
        assertEquals(longValue, labels.get(longKey));
        assertTrue(labels.has("a"));
        assertTrue(labels.has(longKey));
    }

    public void testFromSortedStringsInvalidInput() {
        ByteLabels labels = ByteLabels.fromSortedStrings((String[]) null);
        assertEquals(ByteLabels.emptyLabels(), labels);
        assertTrue(labels.isEmpty());

        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromSortedStrings("k1", "v1", "k2"));
    }
}
