/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.yahoo.pulsar.client.impl.ConsumerImpl;
import com.yahoo.pulsar.client.impl.MessageIdImpl;

public class DispatcherBlockConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(DispatcherBlockConsumerTest.class);

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
    public void testConsumerBlockingWithUnAckedMessagesAtDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerDispatcher();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerDispatcher(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe(topicName, subscriberName, conf);
            Consumer consumer2 = pulsarClient.subscribe(topicName, subscriberName, conf);
            Consumer consumer3 = pulsarClient.subscribe(topicName, subscriberName, conf);
            Consumer[] consumers = { consumer1, consumer2, consumer3 };

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Map<Message, Consumer> messages = Maps.newHashMap();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(msg, consumers[i]);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(messages.size(), unackMsgAllowed, receiverQueueSize*2);

            System.out.println("consumed = "+messages.size());
            // start acknowledging messages
            messages.forEach((m, c) -> {
                try {
                    c.acknowledge(m);
                } catch (PulsarClientException e) {
                    fail("ack failed", e);
                }
            });
            // wait to start dispatching-async
            Thread.sleep(2000);
            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - messages.size();
            for (int i = 0; i < consumers.length; i++) {
                System.out.println("started reconsume for=");
                for (int j = 0; j < remainingMessages; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(msg, consumers[i]);
                        log.info("Received message: " + new String(msg.getData()));
                    }else {
                        break;
                    }
                }
                System.out.println("finished reconsume for=");
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, messages.size());
            producer.close();
            consumer1.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }
   
    
    @Test
    public void testConsumerBlockingWithUnAckedMessagesAndRedelivery() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerDispatcher();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int totalProducedMsgs = 200;
            //final long ackTimeOutSec = 2;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerDispatcher(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Shared);
            //conf.setAckTimeout(ackTimeOutSec, TimeUnit.SECONDS);
            ConsumerImpl consumer1 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer2 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer3 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl[] consumers = { consumer1, consumer2, consumer3 };

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Multimap<ConsumerImpl, Message> messages = ArrayListMultimap.create();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            int totalConsumedMsgs = messages.size();
            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertNotEquals(messages.size(), unackMsgAllowed);

            System.out.println("consumed redelivery = "+messages.size());
            
            // trigger redelivery
            messages.asMap().forEach((c,msgs) -> {
                c.redeliverUnacknowledgedMessages(msgs.stream().map(m -> (MessageIdImpl) m.getMessageId()).collect(Collectors.toSet()));
            });
            
            // wait for redelivery to be completed
            Thread.sleep(1000);
            
            // now, broker must have redelivered all unacked messages
            messages.clear();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }
            
            // check all unacked messages have been redelivered
            assertEquals(totalConsumedMsgs, messages.size());
            
            // start acknowledging messages
            messages.asMap().forEach((c, msgs) -> {
                msgs.forEach(m -> {
                    try {
                        c.acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("ack failed", e);
                    }                    
                });
            });
            
            // now: dispatcher must be unblocked: wait to start dispatching-async
            Thread.sleep(1000);
            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - messages.size();
            for (int i = 0; i < consumers.length; i++) {
                for (int j = 0; j < remainingMessages; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg);
                        log.info("Received message: " + new String(msg.getData()));
                    }else {
                        break;
                    }
                }
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, messages.size());
            producer.close();
            consumer1.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }
    
    @Test
    public void testCloseConsumerBlockedDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerDispatcher();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerDispatcher(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe(topicName, subscriberName, conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Map<Message, Consumer> messages = Maps.newHashMap();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(500, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages.put(msg, consumer1);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(messages.size(), unackMsgAllowed, receiverQueueSize*2);
            
            // create consumer2
            Consumer consumer2 = pulsarClient.subscribe(topicName, subscriberName, conf);
            // close consumer1: all messages of consumer1 must be replayed and received by consumer2
            consumer1.close();
            Map<Message, Consumer> messages2 = Maps.newHashMap();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer2.receive(500, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages2.put(msg, consumer2);
                    consumer2.acknowledge(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            } 

            System.out.println("consumer2 msgs = " + messages2.size());
            assertEquals(messages2.size(), totalProducedMsgs);
            
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }
    
    //@Test
    public void testRedeliveryOnBlockedDistpatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerDispatcher();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerDispatcher(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Shared);
            //conf.setAckTimeout(ackTimeOutSec, TimeUnit.SECONDS);
            ConsumerImpl consumer1 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer2 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer3 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl[] consumers = { consumer1, consumer2, consumer3 };

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Multimap<ConsumerImpl, Message> messages = ArrayListMultimap.create();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            int totalConsumedMsgs = messages.size();
            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertNotEquals(messages.size(), unackMsgAllowed);

            System.out.println("consumed redelivery = "+messages.size());
            
            // trigger redelivery
            messages.asMap().forEach((c,msgs) -> {
                c.redeliverUnacknowledgedMessages();
            });
            
            // wait for redelivery to be completed
            Thread.sleep(1000);
            
            // now, broker must have redelivered all unacked messages
            messages.clear();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }
            
            System.out.println("current consume messages = "+messages.size());
            // check all unacked messages have been redelivered
            assertEquals(totalConsumedMsgs, messages.size());
            
            // start acknowledging messages
            messages.asMap().forEach((c, msgs) -> {
                msgs.forEach(m -> {
                    try {
                        c.acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("ack failed", e);
                    }                    
                });
            });
            
            // now: dispatcher must be unblocked: wait to start dispatching-async
            Thread.sleep(1000);
            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - messages.size();
            for (int i = 0; i < consumers.length; i++) {
                for (int j = 0; j < remainingMessages; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg);
                        log.info("Received message: " + new String(msg.getData()));
                    }else {
                        break;
                    }
                }
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, messages.size());
            producer.close();
            consumer1.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }
}