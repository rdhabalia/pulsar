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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.broker.service.MetadataChangeEvent;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.MetadataChangeEvent.EventType;
import org.apache.pulsar.broker.service.MetadataChangeEvent.ResourceType;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;
import org.apache.pulsar.client.impl.schema.reader.JacksonJsonReader;
import org.apache.pulsar.client.impl.schema.writer.JacksonJsonWriter;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

    /**
     * Verifies: 1. Closing of Broker service unloads all bundle gracefully and there must not be any connected bundles
     * after closing broker service
     *
     * @throws Exception
     */
    @Test
    public void testCloseBrokerService() throws Exception {

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
        Producer<MetadataChangeEvent> producer1 =  pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class)).topic(metadataEventTopic)
                .create();
        
        String ns2 = event.getResourceName()+"-2";
        event.setResourceName(ns2);
        event.setSourceCluster("test2");
        producer1.newMessage().value(event).send();
        
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.namespaces().getPolicies(ns2);
                return true;
            }catch(Exception e) {
                //ok
                return false;
            }
        });
        assertNotNull(admin.namespaces().getPolicies(ns2));
        producer1.close();
        consumer.close();
    }

}
