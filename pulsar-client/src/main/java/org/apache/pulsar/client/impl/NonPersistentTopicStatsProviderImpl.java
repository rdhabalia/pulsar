package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.impl.PersistentTopicStatsProviderImpl.sendRequest;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicStatsProvider;
import org.apache.pulsar.common.api.proto.CommandTopicStats.StatsType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NonPersistentPartitionedTopicStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.TopicInternalStatsInfo;
import org.apache.pulsar.common.policies.data.TopicStatsInfo;
import org.apache.pulsar.common.util.FutureUtil;

public class NonPersistentTopicStatsProviderImpl implements TopicStatsProvider {

    private final PulsarClientImpl client;
    private final ConnectionHandler cnxHandler;
    private final String topicName;

    public NonPersistentTopicStatsProviderImpl(PulsarClientImpl client, ConnectionHandler cnxHandler,
            String topicName) {
        this.client = client;
        this.cnxHandler = cnxHandler;
        this.topicName = topicName;
    }

    @Override
    public CompletableFuture<TopicStatsInfo> getStats() {
        if (TopicName.get(topicName).isPartitioned()) {
            return sendRequest(client, cnxHandler, topicName, StatsType.STATS, NonPersistentPartitionedTopicStats.class)
                    .thenApply(stats -> {
                        TopicStatsInfo info = new TopicStatsInfo(topicName);
                        info.getPartitions().putAll(stats.getPartitions());
                        return info;
                    });
        } else {
            return sendRequest(client, cnxHandler, topicName, StatsType.STATS, NonPersistentTopicStats.class)
                    .thenApply(stats -> {
                        TopicStatsInfo info = new TopicStatsInfo(topicName);
                        info.getPartitions().put(topicName, stats);
                        return info;
                    });
        }
    }

    @Override
    public CompletableFuture<TopicInternalStatsInfo> getInternalStats() {
        return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException("Unsupported method for non-persistent topic"));
    }

}
