/*
 *
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
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

/**
 * One entry returned by the Streaming Lake {@code PAGE_STATS} read API: a page's {@code entryId}
 * paired with its raw per-page range blob (see {@link PageRangeCodec}). The blob is opaque — the
 * bookie returns it verbatim and an index-compaction consumer merges the blobs into segment
 * summaries.
 */
public final class PageStatEntry {

    private final long entryId;
    private final byte[] blob;

    public PageStatEntry(long entryId, byte[] blob) {
        this.entryId = entryId;
        this.blob = blob;
    }

    public long getEntryId() {
        return entryId;
    }

    public byte[] getBlob() {
        return blob;
    }
}
