/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom deserializer for labels in statsd format.
 *
 * Format: "key1:value1,key2:value2"
 *
 * Examples:
 * - "__name__:http_requests_total"
 * - "__name__:http_requests_total,method:GET,status:200"
 */
public class LabelsDeserializer extends JsonDeserializer<Map<String, String>> {

    @Override
    public Map<String, String> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);

        if (!node.isTextual()) {
            throw new IOException("Labels must be a string in statsd format: key1:value1,key2:value2");
        }

        return parseStatsdFormat(node.asText());
    }

    /**
     * Parse statsd-style format: "key1:value1,key2:value2"
     */
    private Map<String, String> parseStatsdFormat(String labelsStr) throws IOException {
        Map<String, String> labels = new HashMap<>();

        if (labelsStr == null || labelsStr.trim().isEmpty()) {
            return labels;
        }

        String[] pairs = labelsStr.split(",");
        for (String pair : pairs) {
            String trimmedPair = pair.trim();
            if (trimmedPair.isEmpty()) {
                continue;
            }

            int colonIndex = trimmedPair.indexOf(':');
            if (colonIndex <= 0 || colonIndex == trimmedPair.length() - 1) {
                throw new IOException(
                    "Invalid label format: '" + trimmedPair + "'. Expected format: 'key:value' or 'key1:value1,key2:value2'"
                );
            }

            String key = trimmedPair.substring(0, colonIndex).trim();
            String value = trimmedPair.substring(colonIndex + 1).trim();

            if (key.isEmpty() || value.isEmpty()) {
                throw new IOException("Label key and value cannot be empty in: '" + trimmedPair + "'");
            }

            labels.put(key, value);
        }

        return labels;
    }
}
