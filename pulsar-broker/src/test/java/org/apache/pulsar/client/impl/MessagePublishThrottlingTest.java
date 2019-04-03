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

import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class MessagePublishThrottlingTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessagePublishThrottlingTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        this.conf.setClusterName("test");
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        super.resetConfig();
    }

    @Test
    public void testSimplePublishMessageThrottling() throws Exception {

        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate publishMsgRate = new PublishRate();
        publishMsgRate.publishThrottlingRateInMsg = 5;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        // (1) verify message-rate is -1 initially
        Assert.assertEquals(topic.getPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.namespaces().setPublishMsgRate(namespace, publishMsgRate);
        retryStrategically((test) -> !topic.getPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER), 5,
                200);
        Assert.assertNotEquals(topic.getPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        int numMessages = 20;

        ClientCnx cnx = producer.getCnx();
        for (int i = 0; i < numMessages; i++) {
            System.out.println("*************** sending *************     " + i);
            producer.send(new byte[80]);
            if (i == publishMsgRate.publishThrottlingRateInMsg) {
                Thread.sleep(conf.getPublisherThrottlingTickTimeMillis());
                // now try to publish a message which will be throttled and producer will get throttling error and
                // it will close the connect and recreate the cnx
                producer.send(new byte[80]);
                ClientCnx currentCnx = producer.getCnx();
                Assert.assertNotEquals(cnx, currentCnx);
                cnx = currentCnx;
            }
        }

        // disable throttling
        publishMsgRate.publishThrottlingRateInMsg = -1;
        admin.namespaces().setPublishMsgRate(namespace, publishMsgRate);
        retryStrategically((test) -> topic.getPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER), 5,
                200);
        Assert.assertEquals(topic.getPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        cnx = producer.getCnx();
        for (int i = 0; i < numMessages; i++) {
            System.out.println("*************** sending *************     " + i);
            producer.send(new byte[80]);
            if (i == publishMsgRate.publishThrottlingRateInMsg) {
                Thread.sleep(conf.getPublisherThrottlingTickTimeMillis());
                // now try to publish a message which will be throttled and producer will get throttling error and
                // it will close the connect and recreate the cnx
                producer.send(new byte[80]);
                ClientCnx currentCnx = producer.getCnx();
                Assert.assertEquals(cnx, currentCnx);
                cnx = currentCnx;
            }
        }

        consumer.close();
        producer.close();
    }

    @Test
    public void testSimplePublishByteThrottling() throws Exception {

        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate publishMsgRate = new PublishRate();
        publishMsgRate.publishThrottlingRateInByte = 10;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        // (1) verify message-rate is -1 initially
        Assert.assertEquals(topic.getPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.namespaces().setPublishMsgRate(namespace, publishMsgRate);
        retryStrategically((test) -> !topic.getPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER), 5,
                200);
        Assert.assertNotEquals(topic.getPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        int numMessages = 3;

        ClientCnx cnx = producer.getCnx();
        System.out.println("cnx = "+cnx);
        for (int i = 0; i < numMessages; i++) {
            System.out.println("*************** sending *************     " + i);
            producer.send(new byte[10]);
            Thread.sleep(conf.getPublisherThrottlingTickTimeMillis());
            // now try to publish a message which will be throttled and producer will get throttling error and
            // it will close the connect and recreate the cnx
            producer.send(new byte[10]);
            ClientCnx currentCnx = producer.getCnx();
            System.out.println("currentCnx = "+currentCnx);
            Assert.assertNotEquals(cnx, currentCnx);
            cnx = currentCnx;
        }

        // disable throttling
        publishMsgRate.publishThrottlingRateInByte = 0;
        admin.namespaces().setPublishMsgRate(namespace, publishMsgRate);
        retryStrategically((test) -> topic.getPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER), 5,
                200);
        Assert.assertEquals(topic.getPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        cnx = producer.getCnx();
        for (int i = 0; i < numMessages; i++) {
            System.out.println("*************** sending *************     " + i);
            producer.send(new byte[80]);
            if (i == publishMsgRate.publishThrottlingRateInMsg) {
                Thread.sleep(conf.getPublisherThrottlingTickTimeMillis());
                // now try to publish a message which will be throttled and producer will get throttling error and
                // it will close the connect and recreate the cnx
                producer.send(new byte[80]);
                ClientCnx currentCnx = producer.getCnx();
                Assert.assertEquals(cnx, currentCnx);
                cnx = currentCnx;
            }
        }

        consumer.close();
        producer.close();
    }

    
}
