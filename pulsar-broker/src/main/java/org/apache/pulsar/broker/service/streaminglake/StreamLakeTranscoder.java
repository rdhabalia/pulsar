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
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;

/**
 * Read-side counterpart of {@link StreamLakeBatcher}: turns a stored StreamLake page entry back
 * into a standard Pulsar batch entry, in place, so ordinary consumers receive the original
 * messages with native batch indices (and acks/redelivery work unchanged).
 *
 * <p>The N stored messages are re-framed under one parent {@link MessageMetadata} with
 * {@code numMessagesInBatch = N}; the consumer client then splits them as a normal batch.
 */
public final class StreamLakeTranscoder {

    private StreamLakeTranscoder() {
    }

    /** Replace every StreamLake page entry in the list with its transcoded standard-batch entry. */
    public static void transcodeInPlace(List<Entry> entries) {
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            if (e == null) {
                continue;
            }
            ByteBuf data = e.getDataBuffer();
            if (data == null || !StreamLakeBatchPage.isPage(data)) {
                continue;
            }
            ByteBuf batch = pageToBatch(data);
            Entry transcoded = EntryImpl.create(e.getLedgerId(), e.getEntryId(), batch);
            batch.release();
            e.release();
            entries.set(i, transcoded);
        }
    }

    private static ByteBuf pageToBatch(ByteBuf pageEntry) {
        List<ByteBuf> messages = StreamLakeBatchPage.decode(pageEntry);
        MessageMetadata parent = new MessageMetadata();
        ByteBuf batchPayload = PulsarByteBufAllocator.DEFAULT.buffer();
        try {
            boolean first = true;
            for (ByteBuf m : messages) {
                // parseMessageMetadata advances m past magic/checksum/metadata to the payload.
                MessageMetadata mm = Commands.parseMessageMetadata(m);
                if (first) {
                    Commands.initBatchMessageMetadata(parent, mm);
                    first = false;
                }
                Commands.serializeSingleMessageInBatchWithPayload(mm, m, batchPayload);
            }
            parent.setNumMessagesInBatch(messages.size());
            return Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, parent, batchPayload);
        } finally {
            batchPayload.release();
            for (ByteBuf m : messages) {
                m.release();
            }
        }
    }
}
