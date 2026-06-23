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

import static org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.PagePruneCallback;

import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.client.BKException;

/**
 * Client-side completion for the Streaming Lake PAGE_PRUNE command. Mirrors
 * {@link GetListOfEntriesOfLedgerCompletion}: it parses the {@code PagePruneResponse}
 * and hands the surviving candidate entryIds back to the caller.
 */
class PagePruneCompletion extends CompletionValue {
    final PagePruneCallback cb;

    public PagePruneCompletion(final CompletionKey key,
                               final PagePruneCallback origCallback,
                               final long ledgerId,
                               PerChannelBookieClient perChannelBookieClient) {
        super("PagePrune", null, ledgerId, 0L, perChannelBookieClient);
        // Reuse the list-of-entries op loggers; PAGE_PRUNE is a read-side metadata query.
        this.opLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionTimeoutOpLogger;
        this.cb = (rc, ledgerId1, entryIds) -> {
            logOpResult(rc);
            origCallback.pagePruneComplete(rc, ledgerId1, entryIds);
            key.release();
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(() -> cb.pagePruneComplete(rc, ledgerId, null));
    }

    @Override
    public void handleV3Response(Response response) {
        PagePruneResponse pruneResponse = response.getPagePruneResponse();
        StatusCode status = response.getStatus() == StatusCode.EOK
                ? pruneResponse.getStatus() : response.getStatus();

        logEvent(status).log("Got page-prune response from bookie");

        int rc = convertStatus(status, BKException.Code.ReadException);
        List<Long> entryIds = null;
        if (rc == BKException.Code.OK) {
            int count = pruneResponse.getEntryIdsCount();
            entryIds = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                entryIds.add(pruneResponse.getEntryIdAt(i));
            }
        }
        cb.pagePruneComplete(rc, ledgerId, entryIds);
    }
}
