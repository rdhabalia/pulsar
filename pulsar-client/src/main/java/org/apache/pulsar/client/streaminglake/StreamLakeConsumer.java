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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Reads StreamLake columnar messages over a raw {@code Consumer<byte[]>}: for each message it strips
 * the payload framing, decodes the Arrow batch into individual records, and returns a {@link
 * StreamLakeRecordBatch} that supports per-record acknowledgement. The inverse of
 * {@link StreamLakeProducer}; the broker performs no transcoding.
 *
 * <p>{@link #close()} releases the Arrow allocator; it does not close the wrapped consumer (the
 * caller owns it).
 */
public final class StreamLakeConsumer implements AutoCloseable {

    private final Consumer<byte[]> consumer;
    private final StreamLakeArrowBatchDecoder decoder;

    public StreamLakeConsumer(Consumer<byte[]> consumer) {
        this.consumer = consumer;
        this.decoder = new StreamLakeArrowBatchDecoder();
    }

    /** Block for the next message and decode it into a StreamLake record batch. */
    public StreamLakeRecordBatch receiveBatch() throws PulsarClientException {
        return toBatch(consumer.receive());
    }

    /** Asynchronously receive and decode the next message. */
    public CompletableFuture<StreamLakeRecordBatch> receiveBatchAsync() {
        return consumer.receiveAsync().thenApply(this::toBatch);
    }

    private StreamLakeRecordBatch toBatch(Message<byte[]> message) {
        byte[] payload = message.getValue();
        boolean framed = StreamLakeBatchPayload.hasFooter(payload);
        byte[] arrow = framed ? StreamLakeBatchPayload.arrowBatch(payload) : payload;
        byte[] footer = framed ? StreamLakeBatchPayload.statsFooter(payload) : null;
        List<Object[]> rows = decoder.decodeRows(arrow);
        return new StreamLakeRecordBatch(consumer, message.getMessageId(), rows, footer);
    }

    @Override
    public void close() {
        decoder.close();
    }
}
