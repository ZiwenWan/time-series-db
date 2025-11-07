/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.tsdb.core.model.Labels;

/**
 * An appender is used to append samples
 */
public interface Appender {
    /**
     * Append a sample to the appender, and create a new series if needed.
     *
     * @param seqNo the sequence number for ordering
     * @param reference the labels hash. This should be a stable hash of the labels. If not available, use labels.stableHash(). Accepting
     *                  an existing hash (e.g. from translog) allows for backwards compatibility as the hash algorithm may change.
     * @param labels the labels
     * @param timestamp the timestamp
     * @param value the value
     * @param failureCallback the callback to execute on failure
     * @return true if a new series was created, false otherwise
     */
    boolean preprocess(long seqNo, long reference, Labels labels, long timestamp, double value, Runnable failureCallback);

    /**
     * Append the sample to the series found or created during preprocessing. Execute a callback under the series lock.
     * This allows for atomic operations that need to be synchronized with append such as writing to the translog.
     * The failure callback is executed in case of any errors.
     *
     * @param callback the callback to execute under the series lock
     * @param failureCallback the callback to execute on failure
     * @return true if appending is successful, false otherwise
     * @throws InterruptedException if the thread is interrupted while waiting for the series lock (append failed)
     */
    boolean append(Runnable callback, Runnable failureCallback) throws InterruptedException;
}
