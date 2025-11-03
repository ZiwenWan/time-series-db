/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import java.util.concurrent.TimeUnit;

/**
 * Constants used throughout the time series database functionality.
 */
public class Constants {

    /**
     * Private constructor to prevent instantiation.
     */
    private Constants() {}

    /**
     * Default units are in milliseconds, but can be overridden using other time units if metric sample input will use a different unit.
     * The time units used for these constants must match the time unit used for metric samples, and be consistent across configs.
     * Changing the time unit for an existing index is not safely supported.
     */
    public static class Time {

        /**
         * Private constructor to prevent instantiation.
         */
        private Time() {}

        /**
         * Default time unit used for sample storage.
         */
        public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    }
}
