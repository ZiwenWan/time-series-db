/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Indexing functionality for closed (completed) time series chunks.
 *
 * This package provides specialized indexing for closed chunks that contain completed time series data.
 * Closed chunks are immutable and optimized for efficient storage and retrieval of historical data.
 *
 * Key components:
 * - Closed chunk indexing utilities
 * - Lucene LeafReader for reading closed chunk index segments
 * - DocValues wrappers for closed chunk metrics
 */

package org.opensearch.tsdb.core.index.closed;