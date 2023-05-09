package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.pulsar.client.api.TopicStatsProvider;
import org.apache.pulsar.common.policies.data.TopicInternalStatsInfo;
import org.apache.pulsar.common.policies.data.TopicStatsInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

public class PartitionedTopicStatsProviderImpl implements TopicStatsProvider {

    private final ConcurrentOpenHashMap<String, TopicStatsProvider> statsProviders;

    public PartitionedTopicStatsProviderImpl(String topicName) {
        this.statsProviders = ConcurrentOpenHashMap.<String, TopicStatsProvider> newBuilder().build();
    }

    public void addStatsProvider(String partitionName, TopicStatsProvider provider) {
        statsProviders.put(partitionName, provider);
    }

    @Override
    public CompletableFuture<TopicStatsInfo> getStats() {
        TopicStatsInfo statsInfo = new TopicStatsInfo();
        return getTopicStats(statsInfo, (provider) -> provider.getStats(),
                (stats) -> {
                    statsInfo.getPartitions().putAll(stats.getPartitions());
                });
    }

    @Override
    public CompletableFuture<TopicInternalStatsInfo> getInternalStats() {
        TopicInternalStatsInfo statsInfo = new TopicInternalStatsInfo();
        return getTopicStats(statsInfo, (provider) -> provider.getInternalStats(),
                (stats) -> statsInfo.getPartitions().putAll(stats.getPartitions()));
    }

    private <S> CompletableFuture<S> getTopicStats(S stats,
            Function<TopicStatsProvider, CompletableFuture<S>> providerStats, Consumer<S> statsUpdater) {
        CompletableFuture<S> statsResult = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger((int) statsProviders.size());
        statsProviders.forEach((partition, provider) -> {
            providerStats.apply(provider).thenAccept(s -> {
                statsUpdater.accept(s);
                if (count.decrementAndGet() == 0) {
                    statsResult.complete(stats);
                }
            }).exceptionally(ex -> {
                // TODO: logging
                statsResult.completeExceptionally(ex.getCause());
                return null;
            });
        });
        return statsResult;

    }
}
