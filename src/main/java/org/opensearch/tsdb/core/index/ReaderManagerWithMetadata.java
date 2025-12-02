/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index;

import org.apache.lucene.index.ReaderManager;

/**
 * Contains a ReaderManager with corresponding metadata
 * @param readerMananger
 * @param minTimestamp minTimestamp of the index this ReaderManager belongs to
 * @param maxTimestamp maxTimestamp of the index this ReaderManager belongs to
 */
public record ReaderManagerWithMetadata(ReaderManager readerMananger, long minTimestamp, long maxTimestamp) {
}
