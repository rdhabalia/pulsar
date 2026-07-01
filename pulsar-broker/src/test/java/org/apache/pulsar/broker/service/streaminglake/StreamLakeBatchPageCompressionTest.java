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
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Page-level round-trip of compressed vs raw column blocks through {@link StreamLakeBatchPage}
 * (no broker/bookie): the read APIs must return identical values whether or not the column data is
 * compressed, the payload region must be untouched, and the compressed page must be smaller.
 */
public class StreamLakeBatchPageCompressionTest {

    private static List<ByteBuf> dummyMessages(int n) {
        List<ByteBuf> msgs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            msgs.add(Unpooled.wrappedBuffer(("payload-" + i).getBytes()));
        }
        return msgs;
    }

    @Test
    public void compressedAndRawReadIdentically() {
        int n = 500;
        int[] ids = {1, 2, 3, 4};
        byte[] types = {StreamLakeBatchPage.TYPE_LONG, StreamLakeBatchPage.TYPE_LONG,
                StreamLakeBatchPage.TYPE_INT, StreamLakeBatchPage.TYPE_INT};
        long[][] values = new long[4][n];
        for (int i = 0; i < n; i++) {
            values[0][i] = 1_700_000_000_000L + i * 1000L; // LONG constant stride -> double-delta
            values[1][i] = 9_000_000_000L + i;             // LONG dense monotonic -> delta
            values[2][i] = i % 4;                          // INT low cardinality -> dict
            values[3][i] = i;                              // INT dense -> delta/for
        }

        ByteBuf raw = StreamLakeBatchPage.encode(dummyMessages(n), ids, types, values,
                0, 0, 64, 0, 64, false);
        ByteBuf packed = StreamLakeBatchPage.encode(dummyMessages(n), ids, types, values,
                0, 0, 64, 0, 64, true);
        try {
            assertEquals(StreamLakeBatchPage.messageCount(raw), n);
            assertEquals(StreamLakeBatchPage.messageCount(packed), n);

            // every column reads back identically from both pages
            int[] scatter = {0, 1, 7, 63, 64, 199, 256, 499};
            for (int c = 0; c < ids.length; c++) {
                long[] expected = new long[n];
                for (int i = 0; i < n; i++) {
                    expected[i] = types[c] == StreamLakeBatchPage.TYPE_LONG ? values[c][i] : (int) values[c][i];
                }
                assertEquals(StreamLakeBatchPage.readColumnRange(packed, ids[c], 0, n), expected,
                        "full column " + ids[c]);
                assertEquals(StreamLakeBatchPage.readColumnRange(raw, ids[c], 0, n),
                        StreamLakeBatchPage.readColumnRange(packed, ids[c], 0, n), "raw vs packed column " + ids[c]);

                long[] expScatter = new long[scatter.length];
                for (int k = 0; k < scatter.length; k++) {
                    expScatter[k] = expected[scatter[k]];
                }
                assertEquals(StreamLakeBatchPage.readColumnAt(packed, ids[c], scatter, scatter.length), expScatter,
                        "scattered column " + ids[c]);
            }

            // payloads (publish order) are identical and intact
            List<ByteBuf> rawMsgs = StreamLakeBatchPage.decode(raw);
            List<ByteBuf> packedMsgs = StreamLakeBatchPage.decode(packed);
            try {
                assertEquals(packedMsgs.size(), n);
                for (int i = 0; i < n; i++) {
                    assertEquals(toBytes(packedMsgs.get(i)), ("payload-" + i).getBytes());
                    assertEquals(toBytes(packedMsgs.get(i)), toBytes(rawMsgs.get(i)));
                }
            } finally {
                rawMsgs.forEach(ByteBuf::release);
                packedMsgs.forEach(ByteBuf::release);
            }

            assertTrue(packed.readableBytes() < raw.readableBytes(),
                    "compressed page must be smaller: packed=" + packed.readableBytes()
                            + " raw=" + raw.readableBytes());
        } finally {
            raw.release();
            packed.release();
        }
    }

    private static byte[] toBytes(ByteBuf b) {
        byte[] out = new byte[b.readableBytes()];
        b.getBytes(b.readerIndex(), out);
        return out;
    }
}
