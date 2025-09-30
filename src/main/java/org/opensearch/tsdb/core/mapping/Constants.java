/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.mapping;

/**
 * Defines the index mapping constants for TSDB engine.
 */
public final class Constants {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private Constants() {
        // Utility class
    }

    /**
     * Constants used in IndexSchema for TSDB mapping.
     */
    public static final class IndexSchema {
        /**
         * Private constructor to prevent instantiation of utility class.
         */
        private IndexSchema() {
            // Utility class
        }

        /**
         * Labels of the time series used by query
         */
        public static final String LABELS = "labels";

        /**
         * Hash of the labels. Not guaranteed to be stable across versions.
         */
        public static final String LABELS_HASH = "labels_hash";

        /**
         * Store the reference to chunks for live series index
         */
        public static final String REFERENCE = "reference";

        /**
         * Store the chunk encoded bytes
         */
        public static final String CHUNK = "chunk";

        /**
         * Minimum timestamp of all data points in the chunk
         */
        public static final String MIN_TIMESTAMP = "min_timestamp";

        /**
         * Maximum timestamp of all data points in the chunk
         */
        public static final String MAX_TIMESTAMP = "max_timestamp";
    }

    /**
     * Constants used in Mapping for TSDB documents.
     */
    public static final class Mapping {
        /**
         * Private constructor to prevent instantiation of utility class.
         */
        private Mapping() {
            // Utility class
        }
    }
}
