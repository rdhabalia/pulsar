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

package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.PageStatsCallback;

import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.ldb.PageStatEntry;
import org.apache.bookkeeper.client.BKException;

/**
 * Client-side completion for the Streaming Lake PAGE_STATS command. Mirrors
 * {@link PagePruneCompletion}: it parses the {@code PageStatsResponse} and hands the raw per-page
 * range blobs (paired with their entryIds) back to the caller for segment-index compaction.
 */
class PageStatsCompletion extends CompletionValue {
    final PageStatsCallback cb;

    public PageStatsCompletion(final CompletionKey key,
                               final PageStatsCallback origCallback,
                               final long ledgerId,
                               PerChannelBookieClient perChannelBookieClient) {
        super("PageStats", null, ledgerId, 0L, perChannelBookieClient);
        // Reuse the list-of-entries op loggers; PAGE_STATS is a read-side metadata query.
        this.opLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionTimeoutOpLogger;
        this.cb = (rc, ledgerId1, stats) -> {
            logOpResult(rc);
            origCallback.pageStatsComplete(rc, ledgerId1, stats);
            key.release();
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(() -> cb.pageStatsComplete(rc, ledgerId, null));
    }

    @Override
    public void handleV3Response(Response response) {
        PageStatsResponse statsResponse = response.getPageStatsResponse();
        StatusCode status = response.getStatus() == StatusCode.EOK
                ? statsResponse.getStatus() : response.getStatus();

        logEvent(status).log("Got page-stats response from bookie");

        int rc = convertStatus(status, BKException.Code.ReadException);
        List<PageStatEntry> stats = null;
        if (rc == BKException.Code.OK) {
            int count = statsResponse.getStatsCount();
            stats = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                PageStat s = statsResponse.getStatAt(i);
                stats.add(new PageStatEntry(s.getEntryId(), s.hasBlob() ? s.getBlob() : new byte[0]));
            }
        }
        cb.pageStatsComplete(rc, ledgerId, stats);
    }
}
