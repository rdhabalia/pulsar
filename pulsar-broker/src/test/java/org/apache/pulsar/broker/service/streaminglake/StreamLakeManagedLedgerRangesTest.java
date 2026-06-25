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
package org.apache.pulsar.broker.service.streaminglake;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.testng.annotations.Test;

/**
 * Slice 2a: the new {@code ManagedLedger.asyncAddEntry(buffer, n, pageRanges, cb, ctx)} overload
 * actually threads the column-range blob down through OpAddEntry to the BK LedgerHandle, so the
 * ranges land in the bookie's page index. Verified on a real bookie via {@code pagePrune}.
 */
public class StreamLakeManagedLedgerRangesTest extends StreamLakeRealBookieTestBase {

    private static final short DEPT = 2;

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private static byte[] pageBlob(int min, int max) {
        Map<Short, PageRangeCodec.Range> m = new HashMap<>();
        m.put(DEPT, new PageRangeCodec.Range(encInt(min), encInt(max), false, false));
        return PageRangeCodec.encodePage(m);
    }

    @Test(timeOut = 120_000)
    public void managedLedgerThreadsRangesToBookie() throws Exception {
        ManagedLedgerConfig cfg = new ManagedLedgerConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32)
                .setPassword("streamlake");
        ManagedLedger ml = pulsar.getDefaultManagedLedgerFactory().open("sl-ranges-ml", cfg);

        // entries 0..4 -> dept range [1,10]; entries 5..9 -> dept range [11,20]
        List<Position> positions = new ArrayList<>();
        for (int e = 0; e < 10; e++) {
            int min = e < 5 ? 1 : 11;
            int max = e < 5 ? 10 : 20;
            ByteBuf buf = Unpooled.wrappedBuffer(("entry-" + e).getBytes());
            CompletableFuture<Position> f = new CompletableFuture<>();
            ml.asyncAddEntry(buf, 1, pageBlob(min, max), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    f.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    f.completeExceptionally(exception);
                }
            }, null);
            positions.add(f.get(30, TimeUnit.SECONDS));
        }

        long ledgerId = positions.get(0).getLedgerId();
        for (Position p : positions) {
            assertEquals(p.getLedgerId(), ledgerId, "test expects all entries in one ledger");
        }
        long minEntry = positions.get(0).getEntryId();
        long maxEntry = positions.get(9).getEntryId();

        // Use a dedicated BK client against the ensemble for the read-side prune.
        org.apache.bookkeeper.conf.ClientConfiguration bkConf =
                new org.apache.bookkeeper.conf.ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        BookKeeper bk = new BookKeeper(bkConf);
        try {
            LedgerHandle rh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, "streamlake".getBytes());
            BookieId bookie = rh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
            BookieClient bookieClient = bk.getClientCtx().getBookieClient();

            // departmentId > 10 -> only entries 5..9 survive bookie pruning
            Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
            pred.put(DEPT, Collections.singletonList(new PageRangeCodec.Range(encInt(10), null, true, false)));
            List<Long> matched = bookieClient
                    .pagePrune(bookie, ledgerId, minEntry, maxEntry, PageRangeCodec.encode(pred))
                    .get(30, TimeUnit.SECONDS);

            List<Long> expected = new ArrayList<>();
            for (int e = 5; e < 10; e++) {
                expected.add(positions.get(e).getEntryId());
            }
            assertEquals(matched, expected, "bookie should prune to the entries written with dept>10 ranges");

            rh.close();
        } finally {
            bk.close();
        }
        ml.close();
    }
}
