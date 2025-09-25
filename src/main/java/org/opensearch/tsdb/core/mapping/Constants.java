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
        public static final String LABELS = "__labels__";

        /**
         * Store the reference to chunks for live series index
         */
        public static final String REFERENCE = "reference";

        /**
         * Store the chunk encoded bytes
         */
        public static final String CHUNK = "chunk";

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
