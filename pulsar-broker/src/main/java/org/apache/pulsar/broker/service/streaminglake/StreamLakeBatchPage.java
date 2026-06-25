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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;

/**
 * A StreamLake page: N messages packed into one bookie entry.
 *
 * <p>The page keeps each message's original {@code headersAndPayload} so the broker can
 * faithfully reconstruct (transcode) the batch on read for normal consumers. The opaque
 * per-column min/max ranges that drive bookie pruning are computed separately
 * ({@link StreamLakeRangeBuilder}) and shipped to the bookie alongside the entry, not stored
 * in this payload — the bookie indexes them, the broker stores the rows.
 *
 * <pre>
 *   magic(4)='SLB1' version(1) flags(1) numMessages(4)
 *   per message: length(4) headersAndPayloadBytes
 * </pre>
 *
 * <p>{@code flags} bit 0 marks the page as columnar-encoded (room for a future Vortex flag).
 */
public final class StreamLakeBatchPage {

    private static final int MAGIC = 0x534C4231; // 'S','L','B','1'
    private static final byte VERSION = 1;
    public static final byte FLAG_COLUMNAR = 0x1;

    private StreamLakeBatchPage() {
    }

    /** True if {@code buf} (read-index preserved) starts with a StreamLake page header. */
    public static boolean isPage(ByteBuf buf) {
        return buf.readableBytes() >= 4 && buf.getInt(buf.readerIndex()) == MAGIC;
    }

    /** Encode N messages into one page buffer. Each element is a message's headersAndPayload. */
    public static ByteBuf encode(List<ByteBuf> messages) {
        int size = 10;
        for (ByteBuf m : messages) {
            size += 4 + m.readableBytes();
        }
        ByteBuf out = Unpooled.buffer(size);
        out.writeInt(MAGIC);
        out.writeByte(VERSION);
        out.writeByte(FLAG_COLUMNAR);
        out.writeInt(messages.size());
        for (ByteBuf m : messages) {
            out.writeInt(m.readableBytes());
            out.writeBytes(m, m.readerIndex(), m.readableBytes());
        }
        return out;
    }

    /** Decode a page buffer back into the original per-message headersAndPayload buffers. */
    public static List<ByteBuf> decode(ByteBuf page) {
        int idx = page.readerIndex();
        int magic = page.getInt(idx);
        if (magic != MAGIC) {
            throw new IllegalArgumentException("not a StreamLake page");
        }
        idx += 4;
        idx += 1; // version
        idx += 1; // flags
        int n = page.getInt(idx);
        idx += 4;
        List<ByteBuf> messages = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int len = page.getInt(idx);
            idx += 4;
            messages.add(page.retainedSlice(idx, len));
            idx += len;
        }
        return messages;
    }

    /** Number of messages in the page without fully decoding it. */
    public static int messageCount(ByteBuf page) {
        return page.getInt(page.readerIndex() + 6);
    }
}
