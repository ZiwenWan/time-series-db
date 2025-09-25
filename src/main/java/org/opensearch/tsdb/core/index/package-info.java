/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Indexing functionality for head storage time series data.
 *
 * This package provides indexing capabilities for both live (active) and closed (completed) time series data
 * within the head storage. It enables efficient querying and retrieval of time series data through
 * Lucene-based indexing structures.
 *
 * Key components:
 * - Common indexing utilities and constants
 * - Integration with Lucene for time series data indexing
 * - Support for both live and closed chunk indexing
 */
package org.opensearch.tsdb.core.index;
