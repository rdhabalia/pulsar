/*
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
package org.apache.pulsar.broker.service.streaminglake;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-namespace async segment-build pipeline (Phase F). When a StreamLake topic's data ledger closes,
 * the owning broker publishes a {@code {dataTopic, dataLedgerId}} build request to a shared per-namespace
 * <b>system topic</b> instead of building the segment inline. A single StreamLake consumer on a
 * <b>failover</b> subscription drains that topic and, for each request, resolves the data topic's
 * {@link StreamLakeSegmentBuilder} and runs {@link StreamLakeSegmentBuilder#buildForLedger(long)} on the
 * broker executor, acking on success and negatively-acking (redelivery) on failure or when the topic is
 * not owned locally. This decouples the (heavy) columnar segment build from the pub-sub broker's hot
 * path and lets a dedicated builder/query tier own segment creation, while the build itself stays
 * idempotent (skips already-segmented ledgers), so redelivery is safe.
 *
 * <p>The request carries only pointers ({@code dataTopic} + {@code dataLedgerId}); the builder reads the
 * ledger's already-durable page-index footers from BookKeeper and writes the segment ledger, so the
 * consumer needs no data payload. Keyed by {@code dataTopic} so a failover subscription preserves
 * per-topic build order.
 */
public final class StreamLakeSegmentBuildQueue implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentBuildQueue.class);

    /** Reserved system-topic short name (per namespace) carrying segment-build requests. */
    public static final String SYSTEM_TOPIC = "__streamlake_segment_build";
    private static final String SUBSCRIPTION = "streamlake-segment-builder";
    private static final char SEP = '\t';

    private final PulsarService pulsar;
    private final String systemTopic;
    private final int partitions;
    private final Function<String, StreamLakeSegmentBuilder> builderResolver;

    private volatile Producer<byte[]> producer;
    private volatile Consumer<byte[]> consumer;
    private volatile boolean closed;

    private StreamLakeSegmentBuildQueue(PulsarService pulsar, NamespaceName ns, int partitions,
            Function<String, StreamLakeSegmentBuilder> builderResolver) {
        this.pulsar = pulsar;
        this.systemTopic = "persistent://" + ns.toString() + "/" + SYSTEM_TOPIC;
        this.partitions = Math.max(1, partitions);
        this.builderResolver = builderResolver;
    }

    /**
     * Create the queue for a namespace: ensure the <b>sharded</b> (partitioned) build topic exists and
     * eagerly start the failover build consumer, so every broker subscribes and the shards spread the
     * build load across brokers. Never throws; a consumer that fails to start is retried on next use.
     */
    public static StreamLakeSegmentBuildQueue create(PulsarService pulsar, NamespaceName ns, int partitions,
            Function<String, StreamLakeSegmentBuilder> builderResolver) {
        StreamLakeSegmentBuildQueue q = new StreamLakeSegmentBuildQueue(pulsar, ns, partitions,
                builderResolver);
        q.ensurePartitionedTopic();
        q.ensureConsumer();
        return q;
    }

    // Create the partitioned system topic (idempotent: ignore "already exists").
    private void ensurePartitionedTopic() {
        try {
            pulsar.getAdminClient().topics().createPartitionedTopic(systemTopic, partitions);
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.ConflictException alreadyExists) {
            // fine -- already created
        } catch (Exception e) {
            log.warn("StreamLake could not create sharded build topic {} ({} partitions): {}",
                    systemTopic, partitions, e.toString());
        }
    }

    /**
     * Publish a build request for {@code (dataTopic, dataLedgerId)}. The returned future completes when
     * the request is durably enqueued (not when the segment is built); the caller falls back to an
     * inline build if it fails.
     */
    public CompletableFuture<Void> publish(String dataTopic, long dataLedgerId, long sizeBytes) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("build queue closed"));
        }
        Producer<byte[]> p;
        try {
            p = ensureProducer();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        byte[] payload = (dataTopic + SEP + dataLedgerId + SEP + sizeBytes)
                .getBytes(StandardCharsets.UTF_8);
        return p.newMessage().key(dataTopic).value(payload).sendAsync().thenApply(id -> null);
    }

    private Producer<byte[]> ensureProducer() throws Exception {
        Producer<byte[]> p = producer;
        if (p == null) {
            synchronized (this) {
                p = producer;
                if (p == null) {
                    PulsarClient client = pulsar.getClient();
                    p = client.newProducer(Schema.BYTES).topic(systemTopic)
                            .enableBatching(true).blockIfQueueFull(true).create();
                    producer = p;
                }
            }
        }
        return p;
    }

    private void ensureConsumer() {
        if (consumer != null || closed) {
            return;
        }
        synchronized (this) {
            if (consumer != null || closed) {
                return;
            }
            try {
                PulsarClient client = pulsar.getClient();
                consumer = client.newConsumer(Schema.BYTES).topic(systemTopic)
                        .subscriptionName(SUBSCRIPTION)
                        .subscriptionType(SubscriptionType.Failover)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .messageListener(this::onBuildRequest)
                        .subscribe();
            } catch (Exception e) {
                log.warn("StreamLake segment-build consumer start failed for {}: {}", systemTopic, e.toString());
            }
        }
    }

    private void onBuildRequest(Consumer<byte[]> c, Message<byte[]> msg) {
        String body = new String(msg.getValue(), StandardCharsets.UTF_8);
        String[] parts = body.split(String.valueOf(SEP), -1);
        if (parts.length < 2 || parts[0].isEmpty()) {
            c.acknowledgeAsync(msg); // malformed: drop
            return;
        }
        String dataTopic = parts[0];
        final long ledgerId;
        final long sizeBytes;
        try {
            ledgerId = Long.parseLong(parts[1]);
            sizeBytes = parts.length > 2 ? Long.parseLong(parts[2]) : 0;
        } catch (NumberFormatException e) {
            c.acknowledgeAsync(msg);
            return;
        }
        StreamLakeSegmentBuilder builder = builderResolver.apply(dataTopic);
        if (builder == null) {
            // Topic not owned/loaded on this broker; redeliver so the owner (or a retry) handles it.
            c.negativeAcknowledge(msg);
            return;
        }
        // Run the heavy build off the listener thread; ack only after it succeeds (idempotent build makes
        // redelivery safe). A failure negatively-acks for retry.
        pulsar.getExecutor().execute(() -> {
            try {
                builder.buildForLedger(ledgerId, sizeBytes);
                c.acknowledgeAsync(msg);
            } catch (Exception e) {
                log.warn("StreamLake async segment build failed for {} ledger {}: {}",
                        dataTopic, ledgerId, e.toString());
                c.negativeAcknowledge(msg);
            }
        });
    }

    @Override
    public void close() {
        closed = true;
        Consumer<byte[]> c = consumer;
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("StreamLake build consumer close failed for {}: {}", systemTopic, e.toString());
            }
        }
        Producer<byte[]> p = producer;
        if (p != null) {
            try {
                p.close();
            } catch (Exception e) {
                log.warn("StreamLake build producer close failed for {}: {}", systemTopic, e.toString());
            }
        }
    }
}
