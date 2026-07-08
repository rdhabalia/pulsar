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

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Framing for a StreamLake message payload: the Arrow IPC batch followed by the binary stats footer,
 * with a fixed trailer so the broker can slice the footer off the tail (without parsing Arrow) to
 * append to the page-index ledger, and the consumer can recover the Arrow bytes to decode.
 *
 * <p>Layout: {@code [arrow IPC][stats footer][footerLength int32][MAGIC 'SLP1']}.
 */
public final class StreamLakeBatchPayload {

    private static final byte[] MAGIC = {'S', 'L', 'P', '1'};
    private static final int TRAILER = 4 + 4; // footerLength(int32) + magic(4)

    private StreamLakeBatchPayload() {
    }

    public static byte[] combine(byte[] arrowIpc, byte[] statsFooter) {
        ByteBuffer bb = ByteBuffer.allocate(arrowIpc.length + statsFooter.length + TRAILER);
        bb.put(arrowIpc);
        bb.put(statsFooter);
        bb.putInt(statsFooter.length);
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
        return Arrays.copyOfRange(payload, 0, end);
    }

    private static int footerLength(byte[] payload) {
        int off = payload.length - TRAILER;
        return ((payload[off] & 0xFF) << 24) | ((payload[off + 1] & 0xFF) << 16)
                | ((payload[off + 2] & 0xFF) << 8) | (payload[off + 3] & 0xFF);
    }
}
