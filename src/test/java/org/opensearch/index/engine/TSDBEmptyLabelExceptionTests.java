/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.test.OpenSearchTestCase;

public class TSDBEmptyLabelExceptionTests extends OpenSearchTestCase {

    public void testConstructorWithMessage() {
        String message = "Labels cannot be empty";
        TSDBEmptyLabelException exception = new TSDBEmptyLabelException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    public void testConstructorWithMessageAndCause() {
        String message = "Labels cannot be empty";
        Throwable cause = new IllegalArgumentException("Invalid labels");
        TSDBEmptyLabelException exception = new TSDBEmptyLabelException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    public void testExceptionCanBeThrown() {
        TSDBEmptyLabelException exception = assertThrows(
            TSDBEmptyLabelException.class,
            () -> { throw new TSDBEmptyLabelException("Empty labels"); }
        );
        assertEquals("Empty labels", exception.getMessage());
    }
}
