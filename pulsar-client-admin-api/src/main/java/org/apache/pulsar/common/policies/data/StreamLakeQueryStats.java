/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.policies.data;

/**
 * Execution metadata for a StreamLake query, returned to the client alongside the result rows (the
 * broker streams it as a trailing NDJSON <b>object</b> after the row arrays, and {@code pulsar-admin}
 * prints it as a footer). All counters are exact broker-side measurements except {@code peakReadBuffer
 * Bytes}, which is the bounded read-ahead high-water estimate.
 *
 * <p>Read-side accounting: {@code rowsRead}/{@code pagesScanned} are what the scan examined,
 * {@code pagesKept} what survived pruning ({@code pagesPruned = scanned - kept}); {@code bytesRead} is
 * data pulled from BookKeeper; {@code rowsReturned}/{@code bytesReturned} is what the query produced.
 */
public class StreamLakeQueryStats {

    private long rowsRead;
    private long rowsReturned;
    private long pagesScanned;
    private long pagesKept;
    private long candidateLedgers;
    private long bytesRead;
    private long bytesReturned;
    private long peakReadBufferBytes;
    private long elapsedMs;

    public StreamLakeQueryStats() {
    }

    public long getRowsRead() {
        return rowsRead;
    }

    public void setRowsRead(long rowsRead) {
        this.rowsRead = rowsRead;
    }

    public long getRowsReturned() {
        return rowsReturned;
    }

    public void setRowsReturned(long rowsReturned) {
        this.rowsReturned = rowsReturned;
    }

    public long getPagesScanned() {
        return pagesScanned;
    }

    public void setPagesScanned(long pagesScanned) {
        this.pagesScanned = pagesScanned;
    }

    public long getPagesKept() {
        return pagesKept;
    }

    public void setPagesKept(long pagesKept) {
        this.pagesKept = pagesKept;
    }

    /** Pages eliminated by pruning ({@code pagesScanned - pagesKept}). */
    public long getPagesPruned() {
        return Math.max(0, pagesScanned - pagesKept);
    }

    public long getCandidateLedgers() {
        return candidateLedgers;
    }

    public void setCandidateLedgers(long candidateLedgers) {
        this.candidateLedgers = candidateLedgers;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getBytesReturned() {
        return bytesReturned;
    }

    public void setBytesReturned(long bytesReturned) {
        this.bytesReturned = bytesReturned;
    }

    public long getPeakReadBufferBytes() {
        return peakReadBufferBytes;
    }

    public void setPeakReadBufferBytes(long peakReadBufferBytes) {
        this.peakReadBufferBytes = peakReadBufferBytes;
    }

    public long getElapsedMs() {
        return elapsedMs;
    }

    public void setElapsedMs(long elapsedMs) {
        this.elapsedMs = elapsedMs;
    }
}
