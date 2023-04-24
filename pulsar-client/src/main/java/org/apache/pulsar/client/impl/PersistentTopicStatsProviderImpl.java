package org.apache.pulsar.client.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicStatsProvider;
import org.apache.pulsar.common.api.proto.CommandTopicStats.StatsType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicInternalStatsInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.TopicStatsInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import io.netty.buffer.ByteBuf;

public class PersistentTopicStatsProviderImpl implements TopicStatsProvider {

    private final PulsarClientImpl client;
    private final ConnectionHandler cnxHandler;
    private final String topicName;

    public PersistentTopicStatsProviderImpl(PulsarClientImpl client, ConnectionHandler cnxHandler, String topicName) {
        this.client = client;
        this.cnxHandler = cnxHandler;
        this.topicName = topicName;
    }

    @Override
    public CompletableFuture<TopicStatsInfo> getStats() {
        if (TopicName.get(topicName).isPartitioned()) {
            return sendRequest(client, cnxHandler, topicName, StatsType.STATS, PartitionedTopicStats.class)
                    .thenApply(stats -> {
                        TopicStatsInfo info = new TopicStatsInfo(topicName);
                        info.getPartitions().putAll(stats.getPartitions());
                        return info;
                    });
        } else {
            return sendRequest(client, cnxHandler, topicName, StatsType.STATS, TopicStats.class).thenApply(stats -> {
                TopicStatsInfo info = new TopicStatsInfo(topicName);
                info.getPartitions().put(topicName, stats);
                return info;
            });
        }
    }

    @Override
    public CompletableFuture<TopicInternalStatsInfo> getInternalStats() {
        if (TopicName.get(topicName).isPartitioned()) {
            return sendRequest(client, cnxHandler, topicName, StatsType.STATS_INTERNAL,
                    PartitionedTopicInternalStats.class).thenApply(stats -> {
                        TopicInternalStatsInfo info = new TopicInternalStatsInfo(topicName);
                        info.getPartitions().putAll(stats.partitions);
                        return info;
                    });
        } else {
            return sendRequest(client, cnxHandler, topicName, StatsType.STATS_INTERNAL,
                    PersistentTopicInternalStats.class).thenApply(stats -> {
                        TopicInternalStatsInfo info = new TopicInternalStatsInfo(topicName);
                        info.getPartitions().put(topicName, stats);
                        return info;
                    });
        }
    }

    public static <T> CompletableFuture<T> sendRequest(PulsarClientImpl client, ConnectionHandler cnxHandler,
            String topicName, StatsType statsType, Class<T> clazz) {
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newStats(topicName, statsType, requestId);
        CompletableFuture<T> result = new CompletableFuture<>();
        ClientCnx cnx = cnxHandler.cnx();
        cnx.newStatsRequest(cmd, requestId).thenAccept(statsData -> {
            try {
                result.complete(ObjectMapperFactory.getMapper().reader().readValue(statsData, clazz));
            } catch (IOException e) {
                // TODO: logging
                result.completeExceptionally(new PulsarClientException.InvalidMessageException(e.getMessage()));
            }
        }).exceptionally(ex -> {
            result.completeExceptionally(new PulsarClientException(ex.getMessage()));
            return null;
        });
        return result;
    }

}
