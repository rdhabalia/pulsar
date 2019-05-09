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
package org.apache.pulsar.client.impl;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MessageChunkingTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessageChunkingTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testLargeMessage() throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false)
                .create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(i * 10);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        List<MessageId> msgIds = Lists.newArrayList();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = createMessagePayload(i * 10);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            msgIds.add(msg.getMessageId());
        }

        pulsar.getBrokerService().updateRates();

        PublisherStats producerStats = topic.getStats().publishers.get(0);
        SubscriptionStats subStats = topic.getStats().subscriptions.entrySet().iterator().next().getValue();

        assertTrue(producerStats.chunkedMessageRate > 0);
        assertEquals(producerStats.chunkedMessageRate, subStats.chuckedMessageRate);

        ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        PositionImpl readPosition = (PositionImpl) mcursor.getReadPosition();

        PositionImpl markDelete = (PositionImpl) mcursor.getMarkDeletedPosition();

        for (MessageId msgId : msgIds) {
            consumer.acknowledge(msgId);
        }

        retryStrategically((test) -> {
            return mcursor.getMarkDeletedPosition().getNext().equals(readPosition);
        }, 5, 200);

        assertEquals(readPosition, mcursor.getMarkDeletedPosition().getNext());

        assertEquals(readPosition.getEntryId(), ((ConsumerImpl) consumer).getAvailablePermits());

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);

    }

    @Test
    public void testLargeMessageTimeOut() throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .ackTimeout(5, TimeUnit.SECONDS).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        for (int i = 0; i < totalMessages; i++) {
            String message = createMessagePayload(i * 10);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = createMessagePayload(i * 10);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        retryStrategically((test) -> consumer.unAckedMessageTracker.messageIdPartitionMap.isEmpty(), 10,
                TimeUnit.SECONDS.toMillis(1));

        msg = null;
        messageSet.clear();
        MessageId lastMsgId = null;
        for (int i = 0; i < totalMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            lastMsgId = msg.getMessageId();
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = createMessagePayload(i * 10);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        PositionImpl readPosition = (PositionImpl) mcursor.getReadPosition();

        consumer.acknowledgeCumulative(lastMsgId);

        retryStrategically((test) -> {
            return mcursor.getMarkDeletedPosition().getNext().equals(readPosition);
        }, 5, 200);

        assertEquals(readPosition, mcursor.getMarkDeletedPosition().getNext());

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);

    }

    @Test
    public void testPublishWithFailure() throws Exception {
        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false)
                .create();

        stopBroker();

        try {
            producer.send(createMessagePayload(100).getBytes());
            fail("should have failed with timeout exception");
        } catch (PulsarClientException.TimeoutException e) {
            // Ok
        }
        producer.close();
    }

    @Test
    public void testMaxPendingChunkMessages() throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(10);
        final int totalMessages = 5;
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final int totalProducers = 5;
        ExecutorService executor = Executors.newFixedThreadPool(totalProducers);

        try {
            ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("my-subscriber-name").acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .maxPendingChuckedMessage(1).autoAckOldestChunkedMessageOnQueueFull(true)
                    .ackTimeout(5, TimeUnit.SECONDS).subscribe();

            ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

            Producer<byte[]>[] producers = new Producer[totalProducers];
            int totalPublishedMessages = totalProducers;
            for (int i = 0; i < totalProducers; i++) {
                producers[i] = producerBuilder.enableChunking(true).enableBatching(false).create();
                int index = i;
                executor.submit(() -> {
                    try {
                        producers[index].send(createMessagePayload(10).getBytes());
                    } catch (PulsarClientException e) {
                        // Ok
                    }
                });
            }

            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

            Message<byte[]> msg = null;
            Set<String> messageSet = Sets.newHashSet();
            for (int i = 0; i < totalMessages; i++) {
                msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    break;
                }
                String receivedMessage = new String(msg.getData());
                log.info("Received message: [{}]", receivedMessage);
                messageSet.add(receivedMessage);
                consumer.acknowledge(msg);
            }

            assertNotEquals(messageSet.size(), totalPublishedMessages);

            ManagedCursorImpl mcursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
            PositionImpl readPosition = (PositionImpl) mcursor.getReadPosition();

            PositionImpl markDelete = (PositionImpl) mcursor.getMarkDeletedPosition();

            retryStrategically((test) -> {
                return mcursor.getMarkDeletedPosition().getNext().equals(readPosition);
            }, 5, 200);

            assertEquals(readPosition, mcursor.getMarkDeletedPosition().getNext());

        } finally {
            executor.shutdown();
        }

    }

    /**
     * Validate that chunking is not supported with batching and non-persistent topic
     * 
     * @throws Exception
     */
    @Test
    public void testInvalidUseCaseForChunking() throws Exception {

        log.info("-- Starting {} test --", methodName);
        this.conf.setMaxMessageSize(5);
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final String nonPersistentTopicName = "non-persistent://my-property/my-ns/my-topic2";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(true).create();

        String message = createMessagePayload(100);
        try {
            producer.send(message.getBytes());
            fail("it should have failied because chunking can't be used with batching enabled");
        } catch (PulsarClientException.InvalidMessageException e) {
            // Ok
        }

        producerBuilder = pulsarClient.newProducer().topic(nonPersistentTopicName);
        producer = producerBuilder.enableChunking(true).create();

        try {
            producer.send(message.getBytes());
            fail("it should have failied because chunking can't be used with non-persistent topic");
        } catch (PulsarClientException.InvalidMessageException e) {
            e.printStackTrace();
        }

        log.info("-- Exiting {} test --", methodName);
    }

    private String createMessagePayload(int size) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < size; i++) {
            str.append(i);
        }
        return str.toString();
    }

}
