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
package com.yahoo.pulsar.broker.service;

import static com.yahoo.pulsar.broker.service.BrokerService.LOOKUP_THROTTLING_PATH;
import static com.yahoo.pulsar.broker.service.BrokerService.THROTTLING_LOOKUP_REQUEST_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.client.impl.ConsumerImpl;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;

/**
 */
public class BrokerServiceThrottlingTest extends BrokerTestBase {


    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Verifies: updating zk-thottling node reflects broker-maxConcurrentLookupRequest and updates semaphore.
     * 
     * @throws Exception
     */
    @Test
    public void testThrottlingLookupRequestSemaphore() throws Exception {
        BrokerService service = pulsar.getBrokerService();
        // create a znode for permits
        upsertLookupPermits(10);
        Thread.sleep(500);
        // get zk node in cache and listner is already registered, so next
        // zk-update will update permits value in brokerService
        Method getPermitZkNodeMethod = BrokerService.class.getDeclaredMethod("getLookupRequestPermits");
        getPermitZkNodeMethod.setAccessible(true);
        getPermitZkNodeMethod.invoke(service);
        // update zknode with permit value
        upsertLookupPermits(0);
        Thread.sleep(500);
        assertEquals(service.lookupRequestSemaphore.get().availablePermits(), 0);
        deleteLookupPermits();
    }

    /**
     * Broker has maxConcurrentLookupRequest = 0 so, it rejects incoming lookup request and it cause consumer creation
     * failure.
     * 
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByBroker0Permit() throws Exception {

        BrokerService service = pulsar.getBrokerService();

        final String topicName = "persistent://prop/usw/my-ns/newTopic";

        com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        String lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();
        PulsarClient pulsarClient = PulsarClient.create(lookupUrl, clientConf);

        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        Consumer consumer = pulsarClient.subscribe(topicName, "mysub", consumerConfig);
        consumer.close();

        // update throttling-permit into zk
        // create a znode for permits
        upsertLookupPermits(10);
        Thread.sleep(500);
        // get zk node in cache and listner is already registered, so next
        // zk-update will update permits value in brokerService
        Method getPermitZkNodeMethod = BrokerService.class.getDeclaredMethod("getLookupRequestPermits");
        getPermitZkNodeMethod.setAccessible(true);
        getPermitZkNodeMethod.invoke(service);
        // update zknode with permit value
        upsertLookupPermits(0);

        try {
            consumer = pulsarClient.subscribe(topicName, "mysub", consumerConfig);
            consumer.close();
            fail("It should fail as throttling should not receive any request");
        } catch (com.yahoo.pulsar.client.api.PulsarClientException.TooManyLookupRequestException e) {
            // ok as throttling set to 0
        }
        deleteLookupPermits();
    }

    /**
     * Verifies: Broker side throttling: 1. concurrent_consumer_creation > maxConcurrentLookupRequest at broker 2. few
     * of the consumer creation must fail with TooManyLookupRequestException.
     * 
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByBroker() throws Exception {

        BrokerService service = pulsar.getBrokerService();

        final String topicName = "persistent://prop/usw/my-ns/newTopic";

        com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        clientConf.setIoThreads(20);
        clientConf.setConnectionsPerBroker(20);
        String lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();
        PulsarClient pulsarClient = PulsarClient.create(lookupUrl, clientConf);

        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        consumerConfig.setSubscriptionType(SubscriptionType.Shared);

        // create a throttling-znode for permits
        upsertLookupPermits(10);
        Thread.sleep(500);
        // get zk node in cache and listner is already registered, so next
        // zk-update will update permits value in brokerService
        Method getPermitZkNodeMethod = BrokerService.class.getDeclaredMethod("getLookupRequestPermits");
        getPermitZkNodeMethod.setAccessible(true);
        getPermitZkNodeMethod.invoke(service);
        // update zknode with permit value: 1 (only 1 concurrent request allows)
        upsertLookupPermits(1);
        Thread.sleep(500);

        List<Consumer> successfulConsumers = Lists.newArrayList();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final int totalConsumers = 20;
        CountDownLatch latch = new CountDownLatch(totalConsumers);
        for (int i = 0; i < totalConsumers; i++) {
            executor.execute(() -> {
                try {
                    successfulConsumers.add(pulsarClient.subscribe(topicName, "mysub", consumerConfig));
                } catch (PulsarClientException.TooManyLookupRequestException e) {
                    // ok
                } catch (Exception e) {
                    fail("it shouldn't failed");
                }
                latch.countDown();
            });
        }
        latch.await();

        for (int i = 0; i < successfulConsumers.size(); i++) {
            successfulConsumers.get(i).close();
        }
        pulsarClient.close();
        assertNotEquals(successfulConsumers.size(), totalConsumers);
        deleteLookupPermits();
    }

    /**
     * This testcase make sure that once consumer lost connection with broker, it always reconnects with broker by
     * retrying on throttling-error exception also. 1. all consumers get connected 2. broker restarts with
     * maxConcurrentLookupRequest = 1 3. consumers reconnect and some get TooManyRequestException and again retries 4.
     * eventually all consumers will successfully connect to broker
     * 
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByBrokerInternalRetry() throws Exception {

        final String topicName = "persistent://prop/usw/my-ns/newTopic";

        com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        clientConf.setIoThreads(20);
        clientConf.setConnectionsPerBroker(20);
        String lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();
        PulsarClient pulsarClient = PulsarClient.create(lookupUrl, clientConf);
        // update permit to allow all consumers to get connect first time
        upsertLookupPermits(100);
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        consumerConfig.setSubscriptionType(SubscriptionType.Shared);
        List<Consumer> consumers = Lists.newArrayList();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final int totalConsumers = 8;
        CountDownLatch latch = new CountDownLatch(totalConsumers);
        for (int i = 0; i < totalConsumers; i++) {
            executor.execute(() -> {
                try {
                    consumers.add(pulsarClient.subscribe(topicName, "mysub", consumerConfig));
                } catch (PulsarClientException.TooManyLookupRequestException e) {
                    // ok
                } catch (Exception e) {
                    fail("it shouldn't failed");
                }
                latch.countDown();
            });
        }
        latch.await();

        stopBroker();
        int actualValue = conf.getMaxConcurrentLookupRequest();
        conf.setMaxConcurrentLookupRequest(1);
        startBroker();
        // wait for consumer to reconnect
        Thread.sleep(3000);

        int totalConnectedConsumers = 0;
        for (int i = 0; i < consumers.size(); i++) {
            if (((ConsumerImpl) consumers.get(i)).isConnected()) {
                totalConnectedConsumers++;
            }
            consumers.get(i).close();

        }
        assertEquals(totalConnectedConsumers, totalConsumers);

        pulsarClient.close();
        conf.setMaxConcurrentLookupRequest(actualValue);
        deleteLookupPermits();
    }

    public void upsertLookupPermits(int permits) throws Exception {
        Map<String, String> throttlingMap = Maps.newHashMap();
        throttlingMap.put(THROTTLING_LOOKUP_REQUEST_KEY, Integer.toString(permits));
        byte[] content = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(throttlingMap);
        if (mockZookKeeper.exists(LOOKUP_THROTTLING_PATH, false) != null) {
            mockZookKeeper.setData(LOOKUP_THROTTLING_PATH, content, -1);
        } else {
            ZkUtils.createFullPathOptimistic(mockZookKeeper, LOOKUP_THROTTLING_PATH, content,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
    
    public void deleteLookupPermits() throws Exception {
        mockZookKeeper.delete(LOOKUP_THROTTLING_PATH, -1);
    }

    
}