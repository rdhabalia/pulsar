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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

/**
 * The decoded rows of one StreamLake columnar message, plus the raw stats footer, with per-record
 * acknowledgement over the single underlying Pulsar message: the message is acked only once every
 * record in the batch has been acked (batch-index-ack semantics for a columnar entry). {@link
 * #ackAll()} acks the whole batch at once.
 */
public final class StreamLakeRecordBatch {

    private final Consumer<byte[]> consumer;
    private final MessageId messageId;
    private final List<Object[]> rows;
    private final byte[] statsFooter;
    private final BitSet pending;

    StreamLakeRecordBatch(Consumer<byte[]> consumer, MessageId messageId, List<Object[]> rows,
            byte[] statsFooter) {
        this.consumer = consumer;
        this.messageId = messageId;
        this.rows = rows;
        this.statsFooter = statsFooter;
        this.pending = new BitSet(rows.size());
        this.pending.set(0, rows.size());
    }

    public int size() {
        return rows.size();
    }

    public List<Object[]> rows() {
        return rows;
    }

    public Object[] row(int index) {
        return rows.get(index);
    }

    /** The raw per-batch stats footer, or {@code null} if the message carried no StreamLake footer. */
    public byte[] statsFooter() {
        return statsFooter;
    }

    public MessageId messageId() {
        return messageId;
    }

    /** Ack one record; acks the underlying message once every record in the batch has been acked. */
    public synchronized CompletableFuture<Void> ackRecord(int index) {
        pending.clear(index);
        if (pending.isEmpty()) {
            return consumer.acknowledgeAsync(messageId);
        }
        return CompletableFuture.completedFuture(null);
    }

    /** Ack the entire batch (the underlying message) at once. */
    public CompletableFuture<Void> ackAll() {
        return consumer.acknowledgeAsync(messageId);
    }

    /** Negatively ack the batch, so the whole columnar message is redelivered. */
    public CompletableFuture<Void> negativeAck() {
        consumer.negativeAcknowledge(messageId);
        return CompletableFuture.completedFuture(null);
    }
}
