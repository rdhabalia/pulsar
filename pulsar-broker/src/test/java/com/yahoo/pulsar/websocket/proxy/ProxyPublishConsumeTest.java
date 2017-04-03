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
package com.yahoo.pulsar.websocket.proxy;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.net.URI;
import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.client.api.ProducerConsumerBase;
import com.yahoo.pulsar.websocket.WebSocketService;
import com.yahoo.pulsar.websocket.service.ProxyServer;
import com.yahoo.pulsar.websocket.service.WebSocketProxyConfiguration;
import com.yahoo.pulsar.websocket.service.WebSocketServiceStarter;

public class ProxyPublishConsumeTest extends ProducerConsumerBase {
    protected String methodName;
    private static final int TEST_PORT = PortManager.nextFreePort();
    private static final String CONSUME_URI = "ws://localhost:" + TEST_PORT + "/ws/consumer/persistent/my-property/use/my-ns/my-topic/my-sub?subscriptionType=Failover";
    private static final String PRODUCE_URI = "ws://localhost:" + TEST_PORT + "/ws/producer/persistent/my-property/use/my-ns/my-topic/";

    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(TEST_PORT);
        config.setClusterName("use");
        config.setGlobalZookeeperServers("dummy-zk-servers");
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterClass
    protected void cleanup() throws Exception {
        super.internalCleanup();
        service.close();
        proxyServer.stop();
        log.info("Finished Cleaning Up Test setup");
    }

    @Test(invocationCount=100, timeOut=10000)
    public void socketTest() throws Exception {
        int retry = 3;
        ExecutorService executor = newFixedThreadPool(1);
        for (int i = 0; i < retry; i++) {
            try {
                executor.submit(() -> {
                    try {
                        asyncSocketTest();
                    } catch (Exception e) {
                        log.error("failed to finish socket-client test", e.getMessage());
                    }
                }).get(2, TimeUnit.SECONDS);
                break;
            } catch (Exception e) {
                e.printStackTrace();
                log.error("failed to close clients ", e);
            }
        }
    }

    public void asyncSocketTest() throws Exception {

        URI consumeUri = URI.create(CONSUME_URI);
        URI produceUri = URI.create(PRODUCE_URI);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient1.start();
            consumeClient2.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
            log.info("Connecting to : {}", consumeUri);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
            Assert.assertTrue(producerFuture.get().isOpen());

            while (consumeSocket1.getReceivedMessagesCount() < 10 && consumeSocket2.getReceivedMessagesCount() < 10) {
                Thread.sleep(10);
            }

            // if the subscription type is exclusive (default), either of the consumer sessions has already been closed
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
            Assert.assertTrue(produceSocket.getBuffer().size() > 0);

            if (consumeSocket1.getBuffer().size() > consumeSocket2.getBuffer().size()) {
                Assert.assertEquals(produceSocket.getBuffer(), consumeSocket1.getBuffer());
            } else {
                Assert.assertEquals(produceSocket.getBuffer(), consumeSocket2.getBuffer());
            }
        } finally {
            consumeClient1.stop();
            consumeClient2.stop();
            produceClient.stop();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTest.class);
}
