/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class TSDBTragicExceptionTests extends OpenSearchTestCase {

    public void testConstructorWithMessage() {
        String message = "Tragic exception occurred";
        TSDBTragicException exception = new TSDBTragicException(message);

        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    public void testConstructorWithMessageAndCause() {
        String message = "Tragic exception in LiveSeriesIndex";
        IOException cause = new IOException("IndexWriter closed");
        TSDBTragicException exception = new TSDBTragicException(message, cause);

        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    public void testExceptionCanBeThrown() {
        TSDBTragicException exception = assertThrows(TSDBTragicException.class, () -> { throw new TSDBTragicException("Tragic error"); });
        assertEquals("Tragic error", exception.getMessage());
    }

    public void testExceptionWithCauseCanBeThrown() {
        IOException ioException = new IOException("Disk failure");
        TSDBTragicException exception = assertThrows(TSDBTragicException.class, () -> {
            throw new TSDBTragicException("Fatal error during write", ioException);
        });
        assertEquals("Fatal error during write", exception.getMessage());
        assertEquals(ioException, exception.getCause());
    }
}
