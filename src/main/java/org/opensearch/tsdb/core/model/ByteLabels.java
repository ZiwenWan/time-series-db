/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.common.hash.MurmurHash3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.opensearch.tsdb.core.model.LabelConstants.COLON_SEPARATOR;
import static org.opensearch.tsdb.core.model.LabelConstants.EMPTY_STRING;
import static org.opensearch.tsdb.core.model.LabelConstants.SPACE_SEPARATOR;

/**
 * ByteLabels implements Labels using a space-efficient flat byte array encoding.
 *
 * <h2>Encoding Format</h2>
 * Labels are stored as a sequence of name-value pairs in a flat byte array:
 * <pre>
 * [name1_len][name1_bytes][value1_len][value1_bytes][name2_len][name2_bytes]...
 * </pre>
 *
 * <h2>Length Encoding</h2>
 * String lengths use variable-length encoding:
 * <ul>
 * <li><strong>Short strings (0-254 bytes):</strong> 1 byte containing the length directly</li>
 * <li><strong>Long strings (255+ bytes):</strong> 4 bytes total - first byte is 255 (marker),
 *     followed by 3 bytes containing the actual length in little-endian format (max 16MB)</li>
 * </ul>
 *
 * TODO: support configurable label name/value length limits
 */
public class ByteLabels implements Labels {
    private final byte[] data;

    private long hash = Long.MIN_VALUE;

    private static final ByteLabels EMPTY = new ByteLabels(new byte[0]);

    /** ThreadLocal cache for TreeMap instances to reduce object allocation during label creation. */
    private static final ThreadLocal<TreeMap<String, String>> TREE_MAP_CACHE = ThreadLocal.withInitial(TreeMap::new);

    private static final char DELIMITER = COLON_SEPARATOR;

    private ByteLabels(byte[] data) {
        this.data = data;
    }

    /**
     * Creates a ByteLabels instance from an array of alternating name-value strings.
     *
     * @param labels an array where even indices are names and odd indices are values
     *               (e.g., "name1", "value1", "name2", "value2")
     * @return a new ByteLabels instance with the given labels
     * @throws IllegalArgumentException if the array length is not even (unpaired labels)
     */
    public static ByteLabels fromStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("Labels must be in pairs");
        }

        TreeMap<String, String> sorted = TREE_MAP_CACHE.get();
        sorted.clear(); // Reuse existing TreeMap
        for (int i = 0; i < labels.length; i += 2) {
            sorted.put(labels[i], labels[i + 1]);
        }

        return encodeLabels(sorted);
    }

    /**
     * Creates a ByteLabels instance from a map of label names to values.
     *
     * @param labelMap a map containing label names as keys and label values as values
     * @return a new ByteLabels instance with the given labels, sorted by name
     */
    public static ByteLabels fromMap(Map<String, String> labelMap) {
        TreeMap<String, String> sorted = TREE_MAP_CACHE.get();
        sorted.clear(); // Reuse existing TreeMap
        sorted.putAll(labelMap);
        return encodeLabels(sorted);
    }

    /**
     * Returns a shared empty ByteLabels instance.
     *
     * @return an empty ByteLabels instance
     */
    public static ByteLabels emptyLabels() {
        return EMPTY;
    }

    private static ByteLabels encodeLabels(TreeMap<String, String> labels) {
        if (labels.isEmpty()) {
            return EMPTY;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                appendEncodedString(baos, entry.getKey());
                appendEncodedString(baos, entry.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode labels", e);
        }
        return new ByteLabels(baos.toByteArray());
    }

    /**
     * Encodes a string using variable-length encoding and appends it to the output stream.
     *
     * <p>Encoding format:
     * <ul>
     * <li>For strings 0-254 bytes: [length_byte][string_bytes]</li>
     * <li>For strings 255+ bytes: [255][len_byte1][len_byte2][len_byte3][string_bytes]</li>
     * </ul>
     *
     * @param baos the output stream to append to
     * @param str the string to encode
     * @throws IOException if writing to the stream fails
     * @throws IllegalArgumentException if the string exceeds 16MB (0xFFFFFF bytes)
     */
    private static void appendEncodedString(ByteArrayOutputStream baos, String str) throws IOException {
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        int length = strBytes.length;

        if (length < 255) {
            baos.write(length); // Write length directly in 1 byte
        } else if (length <= 0xFFFFFF) {
            baos.write(255); // Write 255 as marker byte, next 3 bytes contain actual length
            baos.write(length & 0xFF);
            baos.write((length >> 8) & 0xFF);
            baos.write((length >> 16) & 0xFF);
        } else {
            throw new IllegalArgumentException("String too long: " + length);
        }
        baos.write(strBytes);
    }

    /**
     * Decodes a string from the byte array at the given position.
     *
     * @param data the byte array containing encoded strings
     * @param pos the position to start decoding from
     * @return a DecodedString containing the decoded string and the next position
     * @throws IllegalArgumentException if the data is malformed or truncated
     */
    private static DecodedString decodeString(byte[] data, int pos) {
        if (pos >= data.length) {
            throw new IllegalArgumentException("Index out of bounds");
        }

        int length;
        int nextPos = pos + 1;

        int firstByte = data[pos] & 0xFF;
        if (firstByte == 255) {
            // Extended length encoding (4 bytes total)
            if (pos + 4 > data.length) {
                throw new IllegalArgumentException("Incomplete length encoding");
            }
            length = (data[pos + 1] & 0xFF) | ((data[pos + 2] & 0xFF) << 8) | ((data[pos + 3] & 0xFF) << 16);
            nextPos = pos + 4;
        } else {
            // Short length encoding (1 byte)
            length = firstByte;
        }

        if (nextPos + length > data.length) {
            throw new IllegalArgumentException("String extends beyond data bounds");
        }

        String decoded = new String(data, nextPos, length, StandardCharsets.UTF_8);
        return new DecodedString(decoded, nextPos + length);
    }

    /**
     * Parses string length information without decoding the actual string content.
     * This is more efficient when only position information is needed.
     *
     * @param data the byte array containing encoded data
     * @param pos the position to parse from
     * @return a StringPosition containing the length, data start position, and next position
     */
    private static StringPosition parseStringPos(byte[] data, int pos) {
        int length, nextPos = pos + 1;
        int firstByte = data[pos] & 0xFF;
        if (firstByte == 255) {
            length = (data[pos + 1] & 0xFF) | ((data[pos + 2] & 0xFF) << 8) | ((data[pos + 3] & 0xFF) << 16);
            nextPos = pos + 4;
        } else {
            length = firstByte;
        }

        return new StringPosition(length, nextPos, nextPos + length);
    }

    /**
     * Compares a portion of the data array with a target byte array lexicographically.
     * This avoids string creation during label lookups for better performance.
     *
     * @param data the source byte array
     * @param pos the starting position in the source array
     * @param len the length of data to compare
     * @param target the target byte array to compare against
     * @return negative if data < target, zero if equal, positive if data > target
     */
    private static int compareBytes(byte[] data, int pos, int len, byte[] target) {
        int minLen = Math.min(len, target.length);
        for (int i = 0; i < minLen; i++) {
            int diff = (data[pos + i] & 0xFF) - (target[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return len - target.length;
    }

    /**
     * Record representing a decoded string and the position immediately following it.
     *
     * @param value the decoded string value
     * @param nextPos the position in the byte array after this string's data
     */
    private record DecodedString(String value, int nextPos) {
    }

    /**
     * Record representing the position information for a string in the byte array.
     *
     * @param length the decoded length of the string in bytes
     * @param dataStart the position where the string data begins
     * @param nextPos the position after the string data ends
     */
    private record StringPosition(int length, int dataStart, int nextPos) {
    }

    @Override
    public String toKeyValueString() {
        if (data.length == 0) return EMPTY_STRING;

        StringBuilder sb = new StringBuilder();
        int pos = 0;
        boolean first = true;

        while (pos < data.length) {
            if (!first) sb.append(SPACE_SEPARATOR);
            first = false;

            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;

            sb.append(name.value).append(DELIMITER).append(value.value);
        }

        return sb.toString();
    }

    @Override
    public Map<String, String> toMapView() {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        int pos = 0;

        while (pos < data.length) {
            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;

            result.put(name.value, value.value);
        }

        return result;
    }

    @Override
    public boolean isEmpty() {
        return data.length == 0;
    }

    @Override
    public String get(String name) {
        if (name == null || name.isEmpty()) return "";

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        int pos = 0;

        while (pos < data.length) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());
            pos = valuePos.nextPos();

            int cmp = compareBytes(data, namePos.dataStart(), namePos.length(), nameBytes);
            if (cmp == 0) {
                return new String(data, valuePos.dataStart(), valuePos.length(), StandardCharsets.UTF_8);
            } else if (cmp > 0) {
                break;
            }
        }

        return "";
    }

    @Override
    public boolean has(String name) {
        if (name == null || name.isEmpty()) return false;

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        int pos = 0;

        while (pos < data.length) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());
            pos = valuePos.nextPos();

            int cmp = compareBytes(data, namePos.dataStart(), namePos.length(), nameBytes);
            if (cmp == 0) {
                return true;
            } else if (cmp > 0) {
                break;
            }
        }

        return false;
    }

    @Override
    public long stableHash() {
        if (hash != Long.MIN_VALUE) return hash;

        // TODO: use a faster hash function, the current one is 2-3x slower then the one in prometheus
        hash = MurmurHash3.hash128(data, 0, data.length, 0, new MurmurHash3.Hash128()).hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteLabels)) return false;
        ByteLabels other = (ByteLabels) o;
        return Arrays.equals(this.data, other.data);
    }

    @Override
    public int hashCode() {
        long stableHash = stableHash();
        return Long.hashCode(stableHash);
    }

    @Override
    public Set<String> toIndexSet() {
        TreeSet<String> result = new TreeSet<>();
        int pos = 0;

        while (pos < data.length) {
            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;

            result.add(name.value + DELIMITER + value.value);
        }

        return result;
    }

    @Override
    public String toString() {
        return toKeyValueString();
    }
}
