/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

/**
 * Constants used for label parsing and formatting.
 */
public final class LabelConstants {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private LabelConstants() {
        // Utility class
    }

    /** Constants used in toKeyValueString method */

    /* empty string*/
    public static final String EMPTY_STRING = "";

    /**
     * Space separator character.
     */
    public static final char SPACE_SEPARATOR = ' ';

    /**
     * Label delimiter used to delimit label name and value.
     */
    public static final char LABEL_DELIMITER = ':'; // FIXME: use a special char to properly handle UTF-8 labels

}
