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
package com.yahoo.pulsar.testclient;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.client.impl.ConsumerImpl;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public class SharedSubscriptionStressTest {

    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "--conf-file" }, description = "Configuration file")
        public String confFile;

        @Parameter(description = "persistent://prop/cluster/ns/my-topic", required = true)
        public List<String> destinations;

        @Parameter(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
        public int msgRate = 100;

        @Parameter(names = { "-s", "--size" }, description = "Message size")
        public int msgSize = 1024;

        @Parameter(names = { "-t", "--num-topic" }, description = "Number of topics")
        public int numTopics = 1;

        @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL")
        public String serviceURL;

        @Parameter(names = { "--auth_plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(names = {
                "--auth_params" }, description = "Authentication parameters, e.g., \"key1:val1,key2:val2\"")
        public String authParams;

        @Parameter(names = { "-c",
                "--max-connections" }, description = "Max number of TCP connections to a single broker")
        public int maxConnections = 100;

        @Parameter(names = { "-m",
                "--num-messages" }, description = "Number of messages to publish in total. If 0, it will keep publishing")
        public long numMessages = 0;

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If 0, it will keep publishing")
        public long testTime = 0;
    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf-producer");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        if (arguments.destinations.size() != 1) {
            System.out.println("Only one topic name is allowed");
            jc.usage();
            System.exit(-1);
        }

        if (arguments.confFile != null) {
            Properties prop = new Properties(System.getProperties());
            prop.load(new FileInputStream(arguments.confFile));

            if (arguments.serviceURL == null) {
                arguments.serviceURL = prop.getProperty("brokerServiceUrl");
            }

            if (arguments.serviceURL == null) {
                arguments.serviceURL = prop.getProperty("webServiceUrl");
            }

            // fallback to previous-version serviceUrl property to maintain backward-compatibility
            if (arguments.serviceURL == null) {
                arguments.serviceURL = prop.getProperty("serviceUrl", "http://localhost:8080/");
            }

            if (arguments.authPluginClassName == null) {
                arguments.authPluginClassName = prop.getProperty("authPlugin", null);
            }

            if (arguments.authParams == null) {
                arguments.authParams = prop.getProperty("authParams", null);
            }
        }

        arguments.testTime = TimeUnit.SECONDS.toMillis(arguments.testTime);

        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar perf producer with config: {}", w.writeValueAsString(arguments));

        EventLoopGroup eventLoopGroup;
        if (SystemUtils.IS_OS_LINUX) {
            eventLoopGroup = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                    new DefaultThreadFactory("pulsar-perf-producer"));
        } else {
            eventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                    new DefaultThreadFactory("pulsar-perf-producer"));
        }

        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setConnectionsPerBroker(arguments.maxConnections);
        if (isNotBlank(arguments.authPluginClassName)) {
            clientConf.setAuthentication(arguments.authPluginClassName, arguments.authParams);
        }

        PulsarClient client = new PulsarClientImpl(arguments.serviceURL, clientConf, eventLoopGroup);

        final String topicName = arguments.destinations.get(0);
        final int consumersPerSubscription = 4;
        final String subscriberName1 = "subscriber-1";
        final String subscriberName2 = "subscriber-2";

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        List<ConsumerImpl> subs1 = Lists.newArrayList();
        List<ConsumerImpl> subs2 = Lists.newArrayList();
        for (int i = 0; i < consumersPerSubscription; i++) {
            subs1.add((ConsumerImpl) client.subscribe(topicName, subscriberName1, conf));
            subs2.add((ConsumerImpl) client.subscribe(topicName, subscriberName2, conf));
        }
        Producer producer = client.createProducer(topicName, new ProducerConfiguration());
        ExecutorService executor = Executors.newFixedThreadPool((2 * consumersPerSubscription) + 2);

        AtomicLong prodMsg = new AtomicLong();
        executor.submit(() -> {
            log.info("starting producing msgs on {}", topicName);
            while (true) {
                try {
                    producer.send(("my-message-" + (prodMsg.incrementAndGet())).getBytes());
                    if (prodMsg.get() % 5000 == 0) {
                        log.info("producing msg =" + prodMsg.get());
                    }
                    if (arguments.numMessages > 0 && arguments.numMessages >= prodMsg.get()) {
                        log.info("Finished publishing {} messages.. existing", arguments.numMessages);
                        System.exit(1);
                    }
                } catch (Exception e) {
                    log.warn("Failed to send msg {}", prodMsg.get(), e);
                    Thread.sleep(200);
                }
            }
        });

        AtomicInteger consumerCount = new AtomicInteger(0);
        subs1.forEach(sub1 -> {
            // sub1 started
            final int counsumerId = consumerCount.getAndIncrement();
            final Semaphore subSemaphore1 = new Semaphore(100);
            AtomicLong cons1Msgs = new AtomicLong();
            executor.submit(() -> {
                log.info("starting consuming msgs for sub1 on {}", topicName);
                while (true) {
                    try {
                        subSemaphore1.acquire();

                        try {
                            sub1.receiveAsync().get(30, TimeUnit.SECONDS);
                            if (cons1Msgs.incrementAndGet() % 5000 == 0) {
                                log.info("sub1 {} consumed = {}",counsumerId, cons1Msgs.get());
                            }
                        } catch (Exception e) {
                            log.info("{} is blocked and didn't receive any message so redelivering", subscriberName1);
                            sub1.redeliverUnacknowledgedMessages();
                        }

                        try {
                            sub1.receiveAsync().get(2, TimeUnit.MINUTES);
                        } catch (Exception e) {
                            log.info("{} broker didn't dispatch msg on redelivery.. exiting..", subscriberName1);
                            System.exit(1);
                        }

                        subSemaphore1.release();
                    } catch (Exception e) {
                        log.warn("failed to consume1 {}", subscriberName1, e);
                    }
                }
            });
        });

        consumerCount.set(0);
        subs2.forEach(sub2 -> {
            // sub2 started
            final int counsumerId = consumerCount.getAndIncrement();
            AtomicLong cons2Msgs = new AtomicLong();
            final Semaphore subSemaphore2 = new Semaphore(100);
            executor.submit(() -> {
                log.info("starting consuming msgs for sub2  on {}", topicName);
                List<MessageId> msgIds = Lists.newArrayList();
                while (true) {
                    try {
                        subSemaphore2.acquire();
                        try {
                            msgIds.add(sub2.receiveAsync().get(30, TimeUnit.SECONDS).getMessageId());
                            if (cons2Msgs.incrementAndGet() % 1000 == 0) {
                                log.info("sub2 {} consumed = {}", counsumerId, cons2Msgs.get());
                            }
                        } catch (Exception e) {
                            log.info("Starting acking msgs {}", msgIds.size());
                            for (int i = 0; i < msgIds.size(); i++) {
                                try {
                                    if (i % 5000 == 0) {
                                        log.info("acked {} msg", i);
                                    }
                                    sub2.acknowledge(msgIds.get(i));
                                } catch (Exception e1) {
                                    log.warn("failed to ack", e1);
                                }
                            }
                            msgIds.clear();
                        }
                        subSemaphore2.release();
                        // verification broker is working fine
                        try {
                            sub2.acknowledge(sub2.receiveAsync().get(1, TimeUnit.MINUTES).getMessageId());
                        } catch (Exception e) {
                            log.error(
                                    "Test failed because broker didn't dispatch any message after unblocking dispatcher");
                            System.exit(1);
                        }
                    } catch (Exception e) {
                        log.warn("failed to consume2 {} ", subscriberName2, e);
                    }
                }
            });
        });

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();

        client.close();
    }

    private static final Logger log = LoggerFactory.getLogger(SharedSubscriptionStressTest.class);
}
