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
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming Lake bookie-side processor for {@code PAGE_PRUNE} requests.
 *
 * <p>Delegates to {@code LedgerStorage.giveIndexPages(...)}, which compares the
 * opaque predicate blob against per-page column ranges using order-preserving byte
 * comparison only — the bookie never decodes payloads or interprets schema.
 */
class PagePruneProcessorV3 extends PacketProcessorBaseV3 {

    private static final Logger LOG = LoggerFactory.getLogger(PagePruneProcessorV3.class);

    PagePruneProcessorV3(Request request, BookieRequestHandler requestHandler,
                         BookieRequestProcessor requestProcessor) {
        super(request, requestHandler, requestProcessor);
    }

    @Override
    public void run() {
        final OpStatsLogger statsLogger = requestProcessor.getRequestStats().getReadRequestStats();
        final PagePruneRequest req = request.getPagePruneRequest();
        final long ledgerId = req.getLedgerId();
        final long startEntryId = req.getStartEntryId();
        final long endEntryId = req.getEndEntryId();
        final byte[] predicate = req.hasPredicate() ? req.getPredicate() : new byte[0];

        StatusCode status;
        List<Long> matches = null;
        if (!isVersionCompatible()) {
            status = StatusCode.EBADVERSION;
        } else {
            try {
                matches = requestProcessor.bookie.getLedgerStorage()
                        .giveIndexPages(ledgerId, startEntryId, endEntryId, predicate);
                status = StatusCode.EOK;
            } catch (IOException e) {
                LOG.error("Error processing PAGE_PRUNE request for ledger {} [{}, {}]",
                        ledgerId, startEntryId, endEntryId, e);
                status = StatusCode.EIO;
            }
        }

        final Response response = new Response();
        response.setHeader().copyFrom(getHeader());
        response.setStatus(status);
        final PagePruneResponse pruneResponse = response.setPagePruneResponse();
        pruneResponse.setStatus(status);
        pruneResponse.setLedgerId(ledgerId);
        if (matches != null) {
            for (long entryId : matches) {
                pruneResponse.addEntryId(entryId);
            }
        }
        sendResponse(status, response, statsLogger);
    }
}
