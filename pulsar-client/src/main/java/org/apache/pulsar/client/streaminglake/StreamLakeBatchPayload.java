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
package org.apache.pulsar.client.streaminglake;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;

/**
 * Framing for a StreamLake message payload: the (self-compressed) Arrow IPC batch followed by the
 * binary stats footer, with a fixed trailer so the broker can slice the footer off the tail (without
 * parsing Arrow) to append to the page-index ledger, and the consumer/query tier can recover the
 * Arrow bytes to decode.
 *
 * <p>Layout: {@code [arrow IPC (maybe ZSTD)][stats footer][footerLen int32][arrowRawLen int32]
 * [codec int8][MAGIC 'SLP2']}.
 *
 * <p><b>Why the Arrow region is self-compressed here instead of via Pulsar message compression.</b>
 * The broker's durability barrier slices this stats footer off the entry tail <em>without decoding</em>
 * (see {@link #hasFooter(ByteBuf)} / {@link #statsFooter(ByteBuf)}). If Pulsar compressed the whole
 * payload, the trailing MAGIC would be buried inside the compressed blob and the footer would be
 * invisible to the broker, so no page-index/catalog entry would be written and the data would be
 * unqueryable. We therefore compress only the Arrow bytes here and keep the footer + trailer in the
 * clear, and StreamLake producers must run with {@code compressionType(NONE)} so Pulsar does not
 * re-compress (and re-bury) the tail.
 */
public final class StreamLakeBatchPayload {

    private static final byte[] MAGIC = {'S', 'L', 'P', '2'};
    // footerLen(int32) + arrowRawLen(int32) + codec(int8) + magic(4)
    private static final int TRAILER = 4 + 4 + 1 + MAGIC.length;
    private static final byte CODEC_NONE = 0;
    private static final byte CODEC_ZSTD = 1;
    private static final CompressionCodec ZSTD =
            CompressionCodecProvider.getCompressionCodec(CompressionType.ZSTD);

    private StreamLakeBatchPayload() {
    }

    public static byte[] combine(byte[] arrowIpc, byte[] statsFooter) {
        // Self-compress the (potentially large) Arrow region so the data ledger stays small even with
        // Pulsar message compression off; keep the raw form when it does not shrink (tiny pages).
        byte codec = CODEC_NONE;
        byte[] stored = arrowIpc;
        byte[] compressed = zstd(arrowIpc);
        if (compressed.length < arrowIpc.length) {
            stored = compressed;
            codec = CODEC_ZSTD;
        }
        ByteBuffer bb = ByteBuffer.allocate(stored.length + statsFooter.length + TRAILER);
        bb.put(stored);
        bb.put(statsFooter);
        bb.putInt(statsFooter.length);
        bb.putInt(arrowIpc.length);
        bb.put(codec);
        bb.put(MAGIC);
        return bb.array();
    }

    /** Whether {@code payload} carries a StreamLake stats footer (checks the trailing magic). */
    public static boolean hasFooter(byte[] payload) {
        if (payload.length < TRAILER) {
            return false;
        }
        int off = payload.length - MAGIC.length;
        for (int i = 0; i < MAGIC.length; i++) {
            if (payload[off + i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /** The stats-footer bytes (as consumed by {@link StreamLakeBatchStats#decode(byte[])}). */
    public static byte[] statsFooter(byte[] payload) {
        int footerLen = footerLength(payload);
        int start = payload.length - TRAILER - footerLen;
        return Arrays.copyOfRange(payload, start, payload.length - TRAILER);
    }

    /** The Arrow IPC batch bytes (as consumed by the StreamLake decoder). */
    public static byte[] arrowBatch(byte[] payload) {
        int end = payload.length - TRAILER - footerLength(payload);
        byte[] region = Arrays.copyOfRange(payload, 0, end);
        if (codecOf(payload) == CODEC_ZSTD) {
            return unzstd(region, arrowRawLength(payload));
        }
        return region;
    }

    private static int footerLength(byte[] payload) {
        return readInt(payload, payload.length - TRAILER);
    }

    private static int arrowRawLength(byte[] payload) {
        return readInt(payload, payload.length - TRAILER + 4);
    }

    private static byte codecOf(byte[] payload) {
        return payload[payload.length - MAGIC.length - 1];
    }

    private static int readInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    // ---- Arrow-region compression (see class javadoc: the footer/trailer stay uncompressed) --------

    private static byte[] zstd(byte[] raw) {
        ByteBuf out = ZSTD.encode(Unpooled.wrappedBuffer(raw));
        try {
            byte[] b = new byte[out.readableBytes()];
            out.getBytes(out.readerIndex(), b);
            return b;
        } finally {
            out.release();
        }
    }

    private static byte[] unzstd(byte[] compressed, int rawLen) {
        try {
            ByteBuf out = ZSTD.decode(Unpooled.wrappedBuffer(compressed), rawLen);
            try {
                byte[] b = new byte[out.readableBytes()];
                out.getBytes(out.readerIndex(), b);
                return b;
            } finally {
                out.release();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake Arrow decompression failed", e);
        }
    }

    // ---- ByteBuf tail views (broker slices the footer from a persisted entry without a full copy) --

    /** Whether the tail of a persisted entry carries a StreamLake stats footer (trailing magic). */
    public static boolean hasFooter(ByteBuf entry) {
        int n = entry.readableBytes();
        if (n < TRAILER) {
            return false;
        }
        int magicOff = entry.readerIndex() + n - MAGIC.length;
        for (int i = 0; i < MAGIC.length; i++) {
            if (entry.getByte(magicOff + i) != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /** The stats-footer bytes sliced from the tail of a persisted entry (reads only the tail). */
    public static byte[] statsFooter(ByteBuf entry) {
        int n = entry.readableBytes();
        int trailerOff = entry.readerIndex() + n - TRAILER;
        int footerLen = entry.getInt(trailerOff); // big-endian, matches combine()'s putInt
        byte[] footer = new byte[footerLen];
        entry.getBytes(trailerOff - footerLen, footer);
        return footer;
    }
}
