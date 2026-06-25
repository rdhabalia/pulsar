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
 * A StreamLake page (v2): N messages packed into one bookie entry in column-major layout.
 *
 * <p>Indexed columns are stored contiguously (all departmentId values, then all salary values,
 * ...) so a scan can read just one column to evaluate a predicate, and then decode only the
 * matching rows' payloads -- without touching the others. Each message's original
 * {@code headersAndPayload} is also kept (a "payload column") so the broker can faithfully
 * reconstruct the batch for normal consumers. The opaque min/max ranges that drive bookie
 * pruning are computed separately ({@link StreamLakeRangeBuilder}) and shipped to the bookie.
 *
 * <pre>
 *   magic(4)='SLB2' version(1)=2 flags(1) numMessages(4) numCols(2)
 *   column directory: numCols x [ columnId(2) type(1) dataOffset(4) ]
 *   payloadIndexOffset(4)
 *   ... per column: numMessages values (INT=4, LONG=8) at its dataOffset ...
 *   ... payload index: (numMessages+1) x offset(4) ...
 *   ... payload bytes: each message's headersAndPayload ...
 * </pre>
 *
 * <p>{@code flags} bit 0 marks columnar encoding; bit 1 is reserved for a future Vortex codec.
 */
public final class StreamLakeBatchPage {

    private static final int MAGIC = 0x534C4232; // 'S','L','B','2'
    private static final byte VERSION = 2;
    public static final byte FLAG_COLUMNAR = 0x1;
    public static final byte FLAG_VORTEX = 0x2;

    public static final byte TYPE_INT = 0;
    public static final byte TYPE_LONG = 1;

    private static final int HEADER = 12; // magic+version+flags+numMessages+numCols
    private static final int DIR_ENTRY = 7; // columnId(2)+type(1)+dataOffset(4)

    private StreamLakeBatchPage() {
    }

    /** True if {@code buf} (read index preserved) starts with a StreamLake page header. */
    public static boolean isPage(ByteBuf buf) {
        return buf.readableBytes() >= 4 && buf.getInt(buf.readerIndex()) == MAGIC;
    }

    /**
     * Encode a page.
     *
     * @param messages each message's headersAndPayload
     * @param columnIds indexed column ids
     * @param columnTypes per column type ({@link #TYPE_INT}/{@link #TYPE_LONG})
     * @param columnValues columnValues[c][i] = column c's value for message i (INT stored low 32 bits)
     */
    public static ByteBuf encode(List<ByteBuf> messages, int[] columnIds, byte[] columnTypes,
                                 long[][] columnValues) {
        int n = messages.size();
        int numCols = columnIds.length;

        int dirStart = HEADER;
        int payloadIndexOffsetField = dirStart + numCols * DIR_ENTRY;
        int colDataStart = payloadIndexOffsetField + 4;

        int[] colOffset = new int[numCols];
        int cursor = colDataStart;
        for (int c = 0; c < numCols; c++) {
            colOffset[c] = cursor;
            cursor += n * (columnTypes[c] == TYPE_LONG ? 8 : 4);
        }
        int payloadIndexOffset = cursor;
        cursor += (n + 1) * 4;
        int payloadDataStart = cursor;

        // payload index (absolute offsets) + total size
        int[] payloadOffsets = new int[n + 1];
        int p = payloadDataStart;
        for (int i = 0; i < n; i++) {
            payloadOffsets[i] = p;
            p += messages.get(i).readableBytes();
        }
        payloadOffsets[n] = p;
        int totalSize = p;

        ByteBuf out = Unpooled.buffer(totalSize, totalSize);
        out.writeInt(MAGIC);
        out.writeByte(VERSION);
        out.writeByte(FLAG_COLUMNAR);
        out.writeInt(n);
        out.writeShort(numCols);
        for (int c = 0; c < numCols; c++) {
            out.writeShort(columnIds[c]);
            out.writeByte(columnTypes[c]);
            out.writeInt(colOffset[c]);
        }
        out.writeInt(payloadIndexOffset);
        // column-major data
        for (int c = 0; c < numCols; c++) {
            for (int i = 0; i < n; i++) {
                if (columnTypes[c] == TYPE_LONG) {
                    out.writeLong(columnValues[c][i]);
                } else {
                    out.writeInt((int) columnValues[c][i]);
                }
            }
        }
        // payload index + payloads
        for (int i = 0; i <= n; i++) {
            out.writeInt(payloadOffsets[i]);
        }
        for (ByteBuf m : messages) {
            out.writeBytes(m, m.readerIndex(), m.readableBytes());
        }
        return out;
    }

    public static int messageCount(ByteBuf page) {
        return page.getInt(page.readerIndex() + 6);
    }

    /** Read column {@code columnId}'s values for all rows (INT widened to long). */
    public static long[] readColumn(ByteBuf page, int columnId) {
        int base = page.readerIndex();
        int n = page.getInt(base + 6);
        int numCols = page.getShort(base + 10);
        int dir = base + HEADER;
        for (int c = 0; c < numCols; c++) {
            int entry = dir + c * DIR_ENTRY;
            int id = page.getShort(entry) & 0xFFFF;
            byte type = page.getByte(entry + 2);
            int dataOffset = page.getInt(entry + 3);
            if (id == columnId) {
                long[] values = new long[n];
                int off = base + dataOffset;
                for (int i = 0; i < n; i++) {
                    if (type == TYPE_LONG) {
                        values[i] = page.getLong(off + i * 8);
                    } else {
                        values[i] = page.getInt(off + i * 4);
                    }
                }
                return values;
            }
        }
        throw new IllegalArgumentException("column not found: " + columnId);
    }

    /** Materialize a single message (its headersAndPayload) by row index -- used for selective decode. */
    public static ByteBuf messageAt(ByteBuf page, int index) {
        int base = page.readerIndex();
        int payloadIndexOffset = payloadIndexOffset(page);
        int start = page.getInt(base + payloadIndexOffset + index * 4);
        int end = page.getInt(base + payloadIndexOffset + (index + 1) * 4);
        return page.retainedSlice(base + start, end - start);
    }

    /** Materialize every message (for full-batch transcoding to consumers). */
    public static List<ByteBuf> decode(ByteBuf page) {
        int n = messageCount(page);
        List<ByteBuf> messages = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            messages.add(messageAt(page, i));
        }
        return messages;
    }

    private static int payloadIndexOffset(ByteBuf page) {
        int base = page.readerIndex();
        int numCols = page.getShort(base + 10);
        return page.getInt(base + HEADER + numCols * DIR_ENTRY);
    }
}
