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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.MetadataChangeEvent;
import org.apache.pulsar.broker.service.MetadataChangeEvent.EventType;
import org.apache.pulsar.broker.service.MetadataChangeEvent.ResourceType;
import org.apache.pulsar.broker.service.MetadataPolicySyncer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import lombok.Cleanup;

@Test(groups = "broker-impl")
public class MetadataPolicySyncerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MetadataPolicySyncerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testNamespaceMetadataEventSyncerPublish() throws Exception {
        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        Policies policies = new Policies();
        policies.replication_clusters = Sets.newHashSet("test1", "test2");
        byte[] data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        // (1) create a new namespace
        String ns2 = "my-property/brok-ns2";
        MetadataChangeEvent event = new MetadataChangeEvent();
        event.setResource(ResourceType.Namespaces);
        event.setType(EventType.Created);
        event.setSourceCluster("test2");
        event.setResourceName(ns2);
        event.setData(data);

        @Cleanup
        Producer<MetadataChangeEvent> producer1 = pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).create();

        // (1) create namespace
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.namespaces().getPolicies(ns2);
                return true;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        policies = admin.namespaces().getPolicies(ns2);
        assertNotNull(policies);
        assertEquals(policies.replication_clusters, Sets.newHashSet("test1", "test2"));

        // (2) try to publish : create namespace message again. even should be acked successfully
        policies.lastUpdatedTimestamp += 1;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        long time = pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp;

        // (3) update with old event time
        event.setType(EventType.Modified);
        policies.lastUpdatedTimestamp -= 1;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);

        // (4) update with new event time
        event.setType(EventType.Modified);
        time = policies.lastUpdatedTimestamp + 1;
        policies.lastUpdatedTimestamp = time;
        policies.replication_clusters = Sets.newHashSet("test1", "test2", "test3");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().replication_clusters, Sets.newHashSet("test1", "test2", "test3"));

        // (5) Invalid data
        event.setType(EventType.Modified);
        policies.lastUpdatedTimestamp += 1;
        policies.replication_clusters = Sets.newHashSet("test1", "test2", "test3");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes("invalid-data");
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies which should have not changed
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);

        // (6) timestamp racecondition but pick cluster-name to win the update: local cluster will win
        event.setType(EventType.Modified);
        time = pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp;
        policies.lastUpdatedTimestamp = time;
        policies.replication_clusters = Sets.newHashSet("test1");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setSourceCluster("abc"); // "test" < "abc". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().replication_clusters, Sets.newHashSet("test1", "test2", "test3"));

        // (7) timestamp racecondition but pick cluster-name to win the update: remote cluster will win
        event.setType(EventType.Modified);
        policies.lastUpdatedTimestamp = time + 1;
        policies.replication_clusters = Sets.newHashSet("test1");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setSourceCluster("uvw"); // "test" < "uvw". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataPolicySyncer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time + 1);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().replication_clusters, Sets.newHashSet("test1"));

        producer1.close();
    }

    @Test
    public void testNamespaceMetadataEventSyncer0() throws Exception {

        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        @Cleanup
        Consumer<MetadataChangeEvent> consumer = pulsarClient.newConsumer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).subscriptionName("my-subscriber-name").subscribe();

        // (1) create a new namespace
        final String ns1 = "my-property/brok-ns2";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        // check event generation
        Message<MetadataChangeEvent> msg = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        MetadataChangeEvent event = msg.getValue();
        assertEquals(event.getResource(), ResourceType.Namespaces);
        assertEquals(event.getType(), EventType.Created);
        assertEquals(event.getResourceName(), ns1);
        assertEquals(event.getSourceCluster(), conf.getClusterName());
        assertNotNull(event.getData());

        @Cleanup
        Producer<MetadataChangeEvent> producer1 = pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).create();

        String ns2 = event.getResourceName() + "-2";
        event.setResourceName(ns2);
        event.setSourceCluster("test2");
        producer1.newMessage().value(event).send();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.namespaces().getPolicies(ns2);
                return true;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertNotNull(admin.namespaces().getPolicies(ns2));
        producer1.close();
        consumer.close();
    }

}
