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

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.schema.SchemaCompatibilityStrategy;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

public abstract class AbstractTopic implements Topic {

    protected static final long POLICY_UPDATE_FAILURE_RETRY_TIME_SECONDS = 60;

    protected final String topic;

    // Producers currently connected to this topic
    protected final ConcurrentOpenHashSet<Producer> producers;

    protected final BrokerService brokerService;

    // Prefix for replication cursors
    protected final String replicatorPrefix;

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected volatile boolean isFenced;

    // Timestamp of when this topic was last seen active
    protected volatile long lastActive;

    // Flag to signal that producer of this topic has published batch-message so, broker should not allow consumer which
    // doesn't support batch-message
    protected volatile boolean hasBatchMessagePublished = false;

    protected StatsBuckets addEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);

    // Whether messages published must be encrypted or not in this topic
    protected volatile boolean isEncryptionRequired = false;
    protected volatile SchemaCompatibilityStrategy schemaCompatibilityStrategy =
            SchemaCompatibilityStrategy.FULL;
    // schema validation enforced flag
    protected volatile boolean schemaValidationEnforced = false;
    
    protected volatile PublishRateLimiter publishRateLimiter;

    public AbstractTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        this.producers = new ConcurrentOpenHashSet<>(16, 1);
        this.isFenced = false;
        this.replicatorPrefix = brokerService.pulsar().getConfiguration().getReplicatorPrefix();
        this.lastActive = System.nanoTime();
        Policies policies = null;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and publish throttling will be disabled", topic, e.getMessage());
        }
        updatePublishDispatcher(policies);
    }

    protected boolean isProducersExceeded() {
        Policies policies;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseGet(() -> new Policies());
        } catch (Exception e) {
            policies = new Policies();
        }
        final int maxProducers = policies.max_producers_per_topic > 0 ?
                policies.max_producers_per_topic :
                brokerService.pulsar().getConfiguration().getMaxProducersPerTopic();
        if (maxProducers > 0 && maxProducers <= producers.size()) {
            return true;
        }
        return false;
    }

    protected boolean hasLocalProducers() {
        AtomicBoolean foundLocal = new AtomicBoolean(false);
        producers.forEach(producer -> {
            if (!producer.isRemote()) {
                foundLocal.set(true);
            }
        });

        return foundLocal.get();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).toString();
    }

    @Override
    public ConcurrentOpenHashSet<Producer> getProducers() {
        return producers;
    }


    public BrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public String getName() {
        return topic;
    }

    @Override
    public boolean isEncryptionRequired() {
        return isEncryptionRequired;
    }

    @Override
    public boolean getSchemaValidationEnforced() {
        return schemaValidationEnforced;
    }

    public void markBatchMessagePublished() {
        this.hasBatchMessagePublished = true;
    }

    public String getReplicatorPrefix() {
        return replicatorPrefix;
    }

    @Override
    public CompletableFuture<Boolean> hasSchema() {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .getSchema(id).thenApply(Objects::nonNull);
    }

    @Override
    public CompletableFuture<SchemaVersion> addSchema(SchemaData schema) {
        if (schema == null) {
            return CompletableFuture.completedFuture(SchemaVersion.Empty);
        }

        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .putSchemaIfAbsent(id, schema, schemaCompatibilityStrategy);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema() {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        SchemaRegistryService schemaRegistryService = brokerService.pulsar().getSchemaRegistryService();
        return schemaRegistryService.getSchema(id)
                .thenCompose(schema -> {
                    if (schema != null) {
                        return schemaRegistryService.deleteSchema(id, "");
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> isSchemaCompatible(SchemaData schema) {
        String base = TopicName.get(getName()).getPartitionedTopicName();
        String id = TopicName.get(base).getSchemaName();
        return brokerService.pulsar()
                .getSchemaRegistryService()
                .isCompatible(id, schema, schemaCompatibilityStrategy);
    }

    @Override
    public void recordAddLatency(long latencyUSec) {
        addEntryLatencyStatsUsec.addValue(latencyUSec);
    }
    
    @Override
    public void checkPublishThrottlingRate() {
        this.publishRateLimiter.checkPublishRate();
    }

     @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        this.publishRateLimiter.incrementPublishCount(numOfMessages, msgSizeInBytes);
    }

     @Override
    public void resetPublishCountAndEnableReadIfRequired() {
        if (this.publishRateLimiter.resetPublishCount()) {
            enableProduerRead();
        }
    }

     /**
     * it sets cnx auto-readable if producer's cnx is disabled due to publish-throttling
     */
    protected void enableProduerRead() {
        if (producers != null) {
            producers.forEach(producer -> producer.getCnx().enableCnxAutoRead());
        }
    }

     @Override
    public boolean isPublishRateExceeded() {
        return this.publishRateLimiter.isPublishRateExceeded();
    }

     public PublishRateLimiter getPublishRateLimiter() {
        return publishRateLimiter;
    }

     public void updateMaxPublishRate(Policies policies) {
        updatePublishDispatcher(policies);
    }

     private void updatePublishDispatcher(Policies policies) {
        final String clusterName = brokerService.pulsar().getConfiguration().getClusterName();
        final PublishRate publishRate = policies != null && policies.publish_max_message_rate != null
                ? policies.publish_max_message_rate.get(clusterName)
                : null;
        if (publishRate != null
                && (publishRate.publishThrottlingRateInByte > 0 || publishRate.publishThrottlingRateInMsg > 0)) {
            log.info("Enabling publish rate limiting {} on topic {}", publishRate, this.topic);
            if (this.publishRateLimiter == null
                    || this.publishRateLimiter == PublishRateLimiter.DISABLED_RATE_LIMITER) {
                // create new rateLimiter if rate-limiter is disabled
                this.publishRateLimiter = new PublishRateLimiterImpl(policies, clusterName);
                // lazy init Publish-rateLimiting monitoring if not initialized yet
                this.brokerService.setupPublishRateLimiterMonitor();
            } else {
                this.publishRateLimiter.update(policies, clusterName);
            }
        } else {
            log.info("Disabling publish throttling for {}", this.topic);
            this.publishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
            enableProduerRead();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractTopic.class);
}
