/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.OpenSearchException;

/**
 * Exception thrown when a tragic/fatal error occurs in the TSDB engine that requires
 * the engine to be closed and potentially the shard to be failed.
 */
public class TSDBTragicException extends OpenSearchException {

    public TSDBTragicException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TSDBTragicException(String msg) {
        super(msg);
    }
}
