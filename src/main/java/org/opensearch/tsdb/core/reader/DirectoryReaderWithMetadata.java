/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.DirectoryReader;

/**
 * A record that pairs a DirectoryReader with time-based metadata for efficient pruning.
 * This allows filtering of segments/leaves based on time bounds without reading from the index.
 *
 * @param reader the DirectoryReader for a ClosedChunkIndex
 * @param minTimestamp the minimum timestamp in this index (inclusive)
 * @param maxTimestamp the maximum timestamp in this index (inclusive)
 */
public record DirectoryReaderWithMetadata(DirectoryReader reader, long minTimestamp, long maxTimestamp) {
}
