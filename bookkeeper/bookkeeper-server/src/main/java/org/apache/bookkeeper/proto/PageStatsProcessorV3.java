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

import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.ldb.PageStatEntry;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming Lake bookie-side processor for {@code PAGE_STATS} requests.
 *
 * <p>Delegates to {@code LedgerStorage.scanPageStats(...)}, which range-scans the page index and
 * returns each page's {@code entryId} paired with its raw range blob. The blobs are returned
 * verbatim — the bookie never decodes payloads or interprets schema. An index-compaction consumer
 * merges the blobs into segment summaries.
 */
class PageStatsProcessorV3 extends PacketProcessorBaseV3 {

    private static final Logger LOG = LoggerFactory.getLogger(PageStatsProcessorV3.class);

    PageStatsProcessorV3(Request request, BookieRequestHandler requestHandler,
                         BookieRequestProcessor requestProcessor) {
        super(request, requestHandler, requestProcessor);
    }

    @Override
    public void run() {
        final OpStatsLogger statsLogger = requestProcessor.getRequestStats().getReadRequestStats();
        final PageStatsRequest req = request.getPageStatsRequest();
        final long ledgerId = req.getLedgerId();
        final long startEntryId = req.getStartEntryId();
        final long endEntryId = req.getEndEntryId();

        StatusCode status;
        List<PageStatEntry> stats = null;
        if (!isVersionCompatible()) {
            status = StatusCode.EBADVERSION;
        } else {
            try {
                stats = requestProcessor.bookie.getLedgerStorage()
                        .scanPageStats(ledgerId, startEntryId, endEntryId);
                status = StatusCode.EOK;
            } catch (IOException e) {
                LOG.error("Error processing PAGE_STATS request for ledger {} [{}, {}]",
                        ledgerId, startEntryId, endEntryId, e);
                status = StatusCode.EIO;
            }
        }

        final Response response = new Response();
        response.setHeader().copyFrom(getHeader());
        response.setStatus(status);
        final PageStatsResponse statsResponse = response.setPageStatsResponse();
        statsResponse.setStatus(status);
        statsResponse.setLedgerId(ledgerId);
        if (stats != null) {
            for (PageStatEntry e : stats) {
                statsResponse.addStat().setEntryId(e.getEntryId()).setBlob(e.getBlob());
            }
        }
        sendResponse(status, response, statsLogger);
    }
}
