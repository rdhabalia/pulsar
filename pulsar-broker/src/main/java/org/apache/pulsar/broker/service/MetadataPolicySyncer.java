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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.service.MetadataChangeEvent.EventType;
import org.apache.pulsar.broker.service.MetadataChangeEvent.ResourceType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataPolicySyncer {

    private static final Logger log = LoggerFactory.getLogger(MetadataPolicySyncer.class);

    private PulsarService pulsar;
    private BrokerService brokerService;
    private String topicName;
    protected final PulsarClientImpl client;
    protected volatile Producer<MetadataChangeEvent> producer;
    protected ProducerBuilder<MetadataChangeEvent> producerBuilder;

    protected static final AtomicReferenceFieldUpdater<MetadataPolicySyncer, State> STATE_UPDATER =
    AtomicReferenceFieldUpdater.newUpdater(MetadataPolicySyncer.class, State.class, "state");
    private volatile State state = State.Stopped;
    private volatile boolean isActive = false;
    public static String SUBSCRIPTION_NAME = "metadata-syncer";
    protected final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0,
            TimeUnit.MILLISECONDS);

    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    public MetadataPolicySyncer(PulsarService pulsar) throws PulsarServerException {
        this.pulsar = pulsar;
        this.brokerService = pulsar.getBrokerService();
        this.topicName = pulsar.getConfig().getMetadataSyncEventTopic();
        this.client = (PulsarClientImpl) pulsar.getClient();

        if (StringUtils.isNotBlank(topicName)) {
            @SuppressWarnings("serial")
            ConsumerEventListener listener = new ConsumerEventListener() {
                @Override
                public void becameActive(Consumer<?> consumer, int partitionId) {
                    startProducer();
                    isActive = true;
                }

                @Override
                public void becameInactive(Consumer<?> consumer, int partitionId) {
                    isActive = false;
                    closeProducerAsync();
                }
            };
            ConsumerBuilder<MetadataChangeEvent> consumerBuilder = client
                    .newConsumer(AvroSchema.of(MetadataChangeEvent.class)).topic(topicName)
                    .subscriptionName(SUBSCRIPTION_NAME).ackTimeout(30, TimeUnit.SECONDS)
                    .subscriptionType(SubscriptionType.Failover).messageListener((c, msg) -> {
                        updateMetadata(c, msg);
                    }).consumerEventListener(listener);

            this.producerBuilder = client.newProducer(AvroSchema.of(MetadataChangeEvent.class)) //
                    .topic(topicName).messageRoutingMode(MessageRoutingMode.SinglePartition).enableBatching(false)
                    .sendTimeout(0, TimeUnit.SECONDS);

            registerListeners();
            startProducer();
            startConsumer(consumerBuilder);
        }
    }

    private void updateMetadata(Consumer<MetadataChangeEvent> c, Message<MetadataChangeEvent> msg) {
        if (msg.getValue().getResource() == null) {
            log.info("Metadata change event has null resource {}", msg.getMessageId());
            c.acknowledgeAsync(msg);
            return;
        }

        MetadataChangeEvent event = msg.getValue();
        if (pulsar.getConfig().getClusterName().equals(event.getSourceCluster())) {
            return;
        }
        log.info("received metadata sync event {}", event);
        switch (event.getResource()) {
        case Tenants:
            updateTenantMetadata(event, c, msg);
            break;
        case Namespaces:
            updateNamespaceMetadata(event, c, msg);
            break;
        case TOPIC_PARTITION:
            updatePartitionMetadata(event, c, msg);
            break;
        default:
            log.info("Unknown metadata event type {}, msgId={}", event.getResource(), msg.getMessageId());
        }
    }

    private CompletableFuture<Void> updateNamespaceMetadata(MetadataChangeEvent event, Consumer<MetadataChangeEvent> c,
            Message<MetadataChangeEvent> msg) {

        CompletableFuture<Void> updateResult = new CompletableFuture<>();
        NamespaceName namespaceName;
        Policies policies;
        try {
            namespaceName = NamespaceName.get(event.getResourceName());
            policies = ObjectMapperFactory.getThreadLocal().readValue(event.getData(), Policies.class);
        } catch (Exception e) {
            log.error("Failed to deserialize metadata event {}", topicName, e);
            c.acknowledgeAsync(msg);
            updateResult.completeExceptionally(e);
            return updateResult;
        }

        pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(namespaceName).thenAccept(old -> {
            Policies existingNamespace = old.isPresent() ? old.get() : null;
            CompletableFuture<Void> result = null;
            EventType type = event.getType() != EventType.Synchronize ? event.getType()
                    : (old.isPresent() ? EventType.Modified : EventType.Created);
            switch (type) {
            case Created:
                if (existingNamespace == null) {
                    result = pulsar.getPulsarResources().getNamespaceResources().createPoliciesAsync(namespaceName,
                            policies);
                } else {
                    log.info("skip change-event, namespace {} already exist", namespaceName);
                }
                break;
            case Modified:
                if (existingNamespace != null
                        && ((existingNamespace.lastUpdatedTimestamp < policies.lastUpdatedTimestamp)
                                || (event.getSourceCluster() != null
                                        && existingNamespace.lastUpdatedTimestamp == policies.lastUpdatedTimestamp
                                        && pulsar.getConfig().getClusterName()
                                                .compareTo(event.getSourceCluster()) < 0))) {
                    result = pulsar.getPulsarResources().getNamespaceResources().setPoliciesAsync(namespaceName, __ -> {
                        return policies;
                    });
                } else {
                    log.info("skip change-event with {} for namespace {} updated at {}", policies.lastUpdatedTimestamp,
                            namespaceName, existingNamespace.lastUpdatedTimestamp);
                }
                break;
            case Deleted:
                if (existingNamespace != null && existingNamespace.lastUpdatedTimestamp < event.getEventTime()) {
                    result = pulsar.getPulsarResources().getNamespaceResources().deletePoliciesAsync(namespaceName);
                }
                break;
            default:
                log.info("Skipped unknown namespace update with {}", event);
            }
            if (result != null) {
                result.thenAccept(__ -> {
                    log.info("successfully updated event {}", event);
                    updateResult.complete(null);
                    c.acknowledgeAsync(msg);
                }).exceptionally(ue -> {
                    log.warn("Failed while updating tenant metadata {}", msg.getMessageId(), ue);
                    updateResult.completeExceptionally(ue);
                    return null;
                });
            } else {
                updateResult.complete(null);
                c.acknowledgeAsync(msg);
            }
        }).exceptionally(ex -> {
            log.warn("Failed to update tenant metadata {}", msg.getMessageId(), ex);
            updateResult.completeExceptionally(ex);
            return null;
        });
        return updateResult;
    }

    private CompletableFuture<Void> updateTenantMetadata(MetadataChangeEvent event, Consumer<MetadataChangeEvent> c,
            Message<MetadataChangeEvent> msg) {
        CompletableFuture<Void> updateResult = new CompletableFuture<>();
        String tenantName = event.getResourceName();
        TenantInfoImpl tenant;
        try {
            tenant = ObjectMapperFactory.getThreadLocal().readValue(event.getData(), TenantInfoImpl.class);
        } catch (Exception e) {
            log.error("Failed to deserialize metadata event {}", msg.getTopicName(), e);
            updateResult.completeExceptionally(e);
            c.acknowledgeAsync(msg);
            return updateResult;
        }

        pulsar.getPulsarResources().getTenantResources().getTenantAsync(tenantName).thenAccept(old -> {
            TenantInfoImpl existingTenant = old.isPresent() ? (TenantInfoImpl) old.get() : null;
            CompletableFuture<Void> result = null;
            EventType type = event.getType() != EventType.Synchronize ? event.getType()
                    : (old.isPresent() ? EventType.Modified : EventType.Created);
            switch (type) {
            case Created:
                if (existingTenant == null) {
                    result = pulsar.getPulsarResources().getTenantResources().createTenantAsync(tenantName, tenant);
                }
                break;
            case Modified:
                if (existingTenant != null
                        && ((existingTenant.getLastUpdatedTimestamp() < tenant.getLastUpdatedTimestamp())
                                || (event.getSourceCluster() != null
                                        && (existingTenant.getLastUpdatedTimestamp() == tenant.getLastUpdatedTimestamp()
                                                && pulsar.getConfig().getClusterName()
                                                        .compareTo(event.getSourceCluster()) < 0)))) {
                    result = pulsar.getPulsarResources().getTenantResources().updateTenantAsync(tenantName, __ -> {
                        return tenant;
                    });
                } else {
                    log.info("skip change-event with {} for tenant {} updated at {}", tenant.getLastUpdatedTimestamp(),
                            tenantName, existingTenant.getLastUpdatedTimestamp());
                }
                
                break;
            case Deleted:
                if (existingTenant != null && existingTenant.getLastUpdatedTimestamp() < event.getEventTime()) {
                    result = pulsar.getPulsarResources().getTenantResources().deleteTenantAsync(tenantName);
                }
                break;
            default:
                log.info("Skipped tenant update with {}", event);
            }
            if (result != null) {
                result.thenAccept(__ -> {
                    log.info("successfully updated event {}", event);
                    updateResult.complete(null);
                    c.acknowledgeAsync(msg);
                }).exceptionally(ue -> {
                    log.warn("Failed while updating tenant metadata {}", msg.getMessageId(), ue);
                    updateResult.completeExceptionally(ue);
                    return null;
                });
            } else {
                updateResult.complete(null);
                c.acknowledgeAsync(msg);
            }
        }).exceptionally(ex -> {
            log.warn("Failed to update tenant metadata {}", msg.getMessageId(), ex);
            updateResult.completeExceptionally(ex);
            return null;
        });
        return updateResult;
    }

    private void updatePartitionMetadata(MetadataChangeEvent event, Consumer<MetadataChangeEvent> c,
            Message<MetadataChangeEvent> msg) {
        CompletableFuture<Void> updateResult = new CompletableFuture<>();
        String topic = event.getResourceName();
        PartitionedTopicMetadata partitions;
        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
            partitions = ObjectMapperFactory.getThreadLocal().readValue(event.getData(),
                    PartitionedTopicMetadata.class);
        } catch (Exception e) {
            log.error("Failed to deserialize metadata event {}", event, e);
            c.acknowledgeAsync(msg);
            updateResult.completeExceptionally(e);
            return;
        }

        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(topicName).thenAccept(old -> {
                    PartitionedTopicMetadata existingPartitions = old.isPresent() ? old.get() : null;
                    CompletableFuture<Void> result = null;
                    try {
                        switch (event.getType()) {
                        case Created:
                            if (existingPartitions == null) {
                                result = pulsar.getAdminClient().topics().createPartitionedTopicAsync(topic,
                                        partitions.partitions);
                            }
                            break;
                        case Modified:
                            if (existingPartitions != null
                                    && ((existingPartitions.lastUpdatedTimestamp < partitions.lastUpdatedTimestamp)
                                            || (event.getSourceCluster() != null
                                                    && existingPartitions.lastUpdatedTimestamp == partitions.lastUpdatedTimestamp
                                                    && pulsar.getConfig().getClusterName()
                                                            .compareTo(event.getSourceCluster()) < 0))) {
                                result = pulsar.getAdminClient().topics().updatePartitionedTopicAsync(topic,
                                        partitions.partitions);
                            } else {
                                log.info("skip change-event with {} for partitions {} updated at {}",
                                        partitions.lastUpdatedTimestamp, topic,
                                        existingPartitions.lastUpdatedTimestamp);
                            }
                            break;
                        case Deleted:
                            if (existingPartitions != null
                                    && existingPartitions.lastUpdatedTimestamp < event.getEventTime()) {
                                result = pulsar.getAdminClient().topics().deletePartitionedTopicAsync(topic);
                            }
                            break;
                        default:
                            log.info("Skipped partitioned update with {}", event);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get admin-client while updating partitioned metadata {}", event, e);
                        updateResult.completeExceptionally(e);
                        return;
                    }
                    if (result != null) {
                        result.thenAccept(__ -> {
                            log.info("successfully updated event {}", event);
                            updateResult.complete(null);
                            c.acknowledgeAsync(msg);
                        }).exceptionally(ue -> {
                            log.warn("Failed while updating tenant metadata {}", msg.getMessageId(), ue);
                            updateResult.completeExceptionally(ue);
                            return null;
                        });
                    } else {
                        updateResult.complete(null);
                        c.acknowledgeAsync(msg);
                    }
                }).exceptionally(ex -> {
                    log.warn("Failed to update partitioned metadata {}", msg.getMessageId(), ex);
                    updateResult.completeExceptionally(ex);
                    return null;
                });
    }

    private void startConsumer(ConsumerBuilder<MetadataChangeEvent> consumerBuilder) {
        consumerBuilder.subscribeAsync().thenAccept(consumer -> {
            log.info("successfully created consumer {}", topicName);
        }).exceptionally(ex -> {
            log.warn("failed to start consumer for {}. {}", topicName, ex.getMessage());
            startConsumer(consumerBuilder);
            return null;
        });
    }

    private void registerListeners() {
        pulsar.getPulsarResources().getNamespaceResources().registerListener(n -> {
            String path = n.getPath();
            if (!isActive || NotificationType.ChildrenChanged.equals(n.getType())
                    || !path.startsWith(BaseResources.BASE_POLICIES_PATH)) {
                return;
            }
            ResourceType resourceType = (n.getPath().split("/").length == 3) ? ResourceType.Tenants
                    : ResourceType.Namespaces;
            String resource = (n.getPath().split(BaseResources.BASE_POLICIES_PATH + "/")[1]);
            EventType eventType;
            switch (n.getType()) {
            case Created:
                eventType = EventType.Created;
                break;
            case Modified:
                eventType = EventType.Modified;
                break;
            case Deleted:
                eventType = EventType.Deleted;
                break;
            default:
                return;
            }
            publishAsync(path, resourceType, resource, eventType);
        });
    }

    public boolean isActive() {
        return isActive;
    }

    public void publishAsync(String path, ResourceType resourceType, String resource, EventType type) {
        pulsar.getConfigurationMetadataStore().get(path).thenAccept(result -> {
            if (result.isPresent()) {
                byte[] data = result.get().getValue();
                publishAsync(data, resourceType, resource, type);
            }
        });
    }

    public void publishAsync(byte[] data, ResourceType resourceType, String resource, EventType type) {
        MetadataChangeEvent event = new MetadataChangeEvent(type, resourceType, resource, data,
                pulsar.getConfig().getClusterName(), Instant.now().toEpochMilli());
        producer.newMessage().value(event).sendAsync()
                .thenAccept(__ -> log.info("successfully published metadata change event {}", event))
                .exceptionally(ex -> {
                    log.warn("failed to publish metadata update {}, will retry in {}", topicName,
                            MESSAGE_RATE_BACKOFF_MS, ex);
                    pulsar.getBrokerService().executor().schedule(
                            () -> publishAsync(data, resourceType, resource, type), MESSAGE_RATE_BACKOFF_MS,
                            TimeUnit.MILLISECONDS);
                    return null;
                });

    }
    // This method needs to be synchronized with disconnects else if there is a disconnect followed by startProducer
    // the end result can be disconnect.
    public synchronized void startProducer() {
        if (STATE_UPDATER.get(this) == State.Stopping) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("Waiting for change-event producer to close before attempting to reconnect, retrying in {} s",
                        waitTimeMs / 1000.0);
            }
            // BackOff before retrying
            brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
            return;
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state == State.Started) {
                // Already running
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Change-event producer already running");
                }
            } else {
                log.info("[{}][{} -> {}] Change-event producer already being started. state: {}", state);
            }

            return;
        }

        log.info("[{}] Starting producer", topicName);
        producerBuilder.createAsync().thenAccept(prod -> {
            this.producer = prod;
            startSnapshotScheduler();
            log.info("producer is created successfully {}", topicName);
        }).exceptionally(ex -> {
            if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
                long waitTimeMs = backOff.next();
                log.warn("[{}] Failed to create remote producer ({}), retrying in {} s", topicName, ex.getMessage(),
                        waitTimeMs / 1000.0);

                // BackOff before retrying
                brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
            } else {
                log.warn("[{}] Failed to create remote producer. Replicator state: {}", topicName,
                        STATE_UPDATER.get(this), ex);
            }
            return null;
        });

    }

    private void startSnapshotScheduler() {
        pulsar.getBrokerService().executor().scheduleAtFixedRate(() -> triggerSyncSnapshot(), 30, 30, TimeUnit.SECONDS);
    }

    public void triggerSyncSnapshot() {
        pulsar.getPulsarResources().getTenantResources().listTenantsAsync().thenAccept(tenants -> {
            tenants.forEach(tenant -> {
                publishAsync(TenantResources.getPath(tenant), ResourceType.Tenants, tenant, EventType.Synchronize);
                brokerService.executor().execute(() -> {
                    try {
                        List<String> namespaces = pulsar.getPulsarResources().getTenantResources()
                                .getListOfNamespaces(tenant);
                        namespaces.forEach(ns -> {
                            log.info("trigger {} with namespace {}", NamespaceResources.getPath(ns), ns);
                            publishAsync(NamespaceResources.getPath(ns), ResourceType.Namespaces, ns,
                                    EventType.Synchronize);
                        });
                    } catch (MetadataStoreException e) {
                        log.warn("Failed to get list of namesapces for {}, {}", tenant, e.getMessage());
                    }
                });
            });
        });
    }

    protected synchronized CompletableFuture<Void> closeProducerAsync() {
        if (producer == null) {
            STATE_UPDATER.set(this, State.Stopped);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = producer.closeAsync();
        future.thenRun(() -> {
            STATE_UPDATER.set(this, State.Stopped);
            this.producer = null;
        }).exceptionally(ex -> {
            long waitTimeMs = backOff.next();
            log.warn("[{}] Exception: '{}' occurred while trying to close the producer." + " retrying again in {} s",
                    topicName, ex.getMessage(), waitTimeMs / 1000.0);
            // BackOff before retrying
            brokerService.executor().schedule(this::closeProducerAsync, waitTimeMs, TimeUnit.MILLISECONDS);
            return null;
        });
        return future;
    }

}
