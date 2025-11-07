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
 * Exception thrown when labels is missing or empty. This is a temporary exception and will be removed once out-of-order
 * support is added.
 *
 * TODO: Delete this exception along with custom handling of this error
 */
public class TSDBEmptyLabelException extends OpenSearchException {

    public TSDBEmptyLabelException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TSDBEmptyLabelException(String msg) {
        super(msg);
    }
}
