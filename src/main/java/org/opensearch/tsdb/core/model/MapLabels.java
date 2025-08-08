/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.tsdb.core.model;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.hash.MurmurHash3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * MapLabels implements Labels using a HashMap for storage.
 */
public class MapLabels implements Labels {
    private final Map<String, String> labels;
    private long hash = Long.MIN_VALUE;

    public MapLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public static MapLabels fromStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("Labels must be in pairs");
        }
        Map<String, String> labelMap = new HashMap<>();
        for (int i = 0; i < labels.length; i += 2) {
            labelMap.put(labels[i], labels[i + 1]);
        }
        return new MapLabels(labelMap);
    }

    public static MapLabels fromSerializedBytes(byte[] bytes) {
        Map<String, String> labelMap = new HashMap<>();
        try {
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            while (in.getPosition() < bytes.length) {
                String key = in.readString();
                String value = in.readString();
                labelMap.put(key, value);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize labels", e);
        }

        return new MapLabels(labelMap);
    }

    public static MapLabels emptyLabels() {
        return new MapLabels(Map.of());
    }

    @Override
    public String toKeyValueString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(entry.getValue());
            sb.append(' ');
        }
        // Remove the trailing space if there are any labels
        if (!sb.isEmpty()) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public Map<String, String> toMapView() {
        return Map.copyOf(labels);
    }

    @Override
    public boolean isEmpty() {
        return labels.isEmpty();
    }

    @Override
    public int bytes(byte[] bytes) throws IOException {
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        return out.getPosition();
    }

    @Override
    public String get(String name) {
        return labels.getOrDefault(name, "");
    }

    @Override
    public boolean has(String name) {
        return labels.containsKey(name);
    }

    @Override
    public long stableHash() {
        if (hash != Long.MIN_VALUE) {
            return hash;
        }

        // combine logic from boost::hash_combine
        long combinedHash = 0;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            byte[] bytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            long hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128()).hashCode();
            combinedHash ^= (hash + 0x9e3779b97f4a7c15L + (combinedHash << 6) + (combinedHash >> 2));
            bytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
            hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128()).hashCode();
            combinedHash ^= (hash + 0x9e3779b97f4a7c15L + (combinedHash << 6) + (combinedHash >> 2));
        }
        hash = combinedHash;
        return combinedHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapLabels)) return false;
        MapLabels other = (MapLabels) o;
        return Objects.equals(this.labels, other.labels);
    }

    @Override
    public int hashCode() {
        long stableHash = stableHash();
        return Long.hashCode(stableHash);
    }

    @Override
    public String toString() {
        return toKeyValueString();
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(MapLabels.class) + RamUsageEstimator.sizeOfObject(labels);
    }
}
