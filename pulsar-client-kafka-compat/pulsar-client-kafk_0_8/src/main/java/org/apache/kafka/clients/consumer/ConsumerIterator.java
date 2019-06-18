/**
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
package org.apache.kafka.clients.consumer;

import java.util.Base64;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerIterator<K, V> implements Iterator<PulsarMessageAndMetadata<K, V>> {

    private final Consumer<byte[]> consumer;
    private final ConcurrentLinkedQueue<Message<byte[]>> receivedMessages;
    private final Optional<Decoder<K>> keyDeSerializer;
    private final Optional<Decoder<V>> valueDeSerializer;
    private final boolean isAutoCommit;
    private final SubscriptionInitialPosition strategy;
    private volatile MessageId lastConsumedMessageId;

    public ConsumerIterator(Consumer<byte[]> consumer, ConcurrentLinkedQueue<Message<byte[]>> receivedMessages,
            Optional<Decoder<K>> keyDeSerializer, Optional<Decoder<V>> valueDeSerializer, boolean isAutoCommit,
            SubscriptionInitialPosition strategy) {
        this.consumer = consumer;
        this.receivedMessages = receivedMessages;
        this.keyDeSerializer = keyDeSerializer;
        this.valueDeSerializer = valueDeSerializer;
        this.isAutoCommit = isAutoCommit;
        this.strategy = strategy;
        resetOffsets(consumer, strategy);
        // TODO: all configuration strategy
    }

    @Override
    public boolean hasNext() {
        try {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.MILLISECONDS);
            if (msg != null) {
                receivedMessages.offer(msg);
            }
        } catch (PulsarClientException e) {
            // TODO: log and return false
        }
        return false;
    }

    @Override
    public PulsarMessageAndMetadata<K, V> next() {

        Message<byte[]> msg = receivedMessages.poll();
        if (msg == null) {
            try {
                msg = consumer.receive();
            } catch (PulsarClientException e) {
                // TODO: log and throw runtimeException
            }
        }

        int partition = 0; // TODO : get partition from the topic name
        long offset = MessageIdUtils.getOffset(msg.getMessageId());
        String key = msg.getKey();
        byte[] value = msg.getValue();

        // TODO: remove key if not needed
        if (StringUtils.isNotBlank(key)) {
            K desKey = null;
            if (keyDeSerializer.isPresent() && keyDeSerializer.get() instanceof StringDecoder) {
                desKey = (K) key;
            } else {
                byte[] decodedBytes = Base64.getDecoder().decode(key);
                desKey = keyDeSerializer.isPresent() ? keyDeSerializer.get().fromBytes(decodedBytes)
                        : SerializationUtils.deserialize(decodedBytes);
            }
        }

        V desValue = null;
        if (value != null) {
            desValue = valueDeSerializer.isPresent() ? valueDeSerializer.get().fromBytes(msg.getData())
                    : SerializationUtils.deserialize(msg.getData());
        }

        PulsarMessageAndMetadata<K, V> msgAndMetadata = new PulsarMessageAndMetadata<>(consumer.getTopic(), partition,
                null, offset, keyDeSerializer.orElse(null), valueDeSerializer.orElse(null), desValue);

        if (isAutoCommit) {
            // Commit the offset of previously dequeued messages
            consumer.acknowledgeCumulativeAsync(msg);
        }

        lastConsumedMessageId = msg.getMessageId();
        return msgAndMetadata;
    }

    protected CompletableFuture<Void> commitOffsets() {
        MessageId msgId = lastConsumedMessageId;
        if (msgId != null) {
            return this.consumer.acknowledgeCumulativeAsync(msgId);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void resetOffsets(Consumer<byte[]> consumer, SubscriptionInitialPosition strategy) {
        if (strategy == null) {
            return;
        }
        log.info("Resetting partition {} for group-id {} and seeking to {} position", consumer.getTopic(),
                consumer.getSubscription(), strategy);
        try {
            if (strategy == SubscriptionInitialPosition.Earliest) {
                consumer.seek(MessageId.earliest);
            } else {
                consumer.seek(MessageId.latest);
            }
        } catch (PulsarClientException e) {
            // TODO: log and throw runtimeException
        }
    }
}
