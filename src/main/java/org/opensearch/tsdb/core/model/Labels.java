/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.tsdb.core.model;

import java.io.IOException;
import java.util.Map;

/**
 * Labels is a set of name/value pairs.
 */
public interface Labels {

    /**
     * Convert to key:value string format
     * @return string representation
     */
    String toKeyValueString();

    /**
     * Get a read-only map view of the labels
     * @return map view
     */
    Map<String, String> toMapView();

    /**
     * Check if labels are empty
     * @return true if empty
     */
    boolean isEmpty();

    /**
     * Serialize labels to byte array
     * @param bytes output buffer
     * @return number of bytes written
     * @throws IOException if serialization fails
     */
    int bytes(byte[] bytes) throws IOException;

    /**
     * Get stable hash of labels
     * @return stable hash value
     */
    long stableHash();

    /**
     * Get the value for a label name
     * @param name label name
     * @return label value or empty string if not found
     */
    String get(String name);

    /**
     * Check if a label exists
     * @param name label name
     * @return true if label exists
     */
    boolean has(String name);

    /**
     * Calculate the RAM bytes used by this Labels instance.
     * @return the number of bytes used in RAM
     */
    long ramBytesUsed();
}
