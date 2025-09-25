/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Indexing functionality for live series index.
 * <p>This package contains classes that provide read access to live time series
 * data that hasn't been persisted to disk yet, allowing queries to in-memory data.</p>
 *
 * Key components:
 * - Live series index reader implementations
 * - DocValues wrappers for live series metrics
 */
package org.opensearch.tsdb.core.index.live;