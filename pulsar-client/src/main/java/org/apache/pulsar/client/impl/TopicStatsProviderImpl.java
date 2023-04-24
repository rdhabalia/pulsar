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
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import io.netty.buffer.ByteBuf;

public class TopicStatsProviderImpl implements TopicStatsProvider {

    private final PulsarClientImpl client;
    private final ConnectionHandler cnxHandler;
    private final String topicName;

    public TopicStatsProviderImpl(PulsarClientImpl client, ConnectionHandler cnxHandler, String topicName) {
        this.client = client;
        this.cnxHandler = cnxHandler;
        this.topicName = topicName;
    }

    @Override
    public CompletableFuture<TopicStats> getStats() {
        if (TopicName.get(topicName).isPartitioned()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException(
                    topicName + " is partitioned topic : use getPartitionTopicStats() method"));
        }
        return sendRequest(StatsType.STATS, TopicStats.class);
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStats() {
        if (TopicName.get(topicName).isPartitioned()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException(
                    topicName + " is partitioned topic : use getInternalPartitionTopicStats() method"));
        }
        return sendRequest(StatsType.STATS_INTERNAL, PersistentTopicInternalStats.class);
    }

    @Override
    public CompletableFuture<PartitionedTopicStats> getPartitionTopicStats() {
        if (!TopicName.get(topicName).isPartitioned()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException(
                    topicName + " is not partitioned topic : use getStats() method"));
        }
        return sendRequest(StatsType.STATS, PartitionedTopicStats.class);
    }

    @Override
    public CompletableFuture<PartitionedTopicInternalStats> getInternalPartitionTopicStats() {
        if (!TopicName.get(topicName).isPartitioned()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException(
                    topicName + " is not partitioned topic : use getInternalStats() method"));
        }
        return sendRequest(StatsType.STATS_INTERNAL, PartitionedTopicInternalStats.class);
    }

    private <T> CompletableFuture<T> sendRequest(StatsType statsType, Class<T> clazz) {
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
        }).exceptionally(ex ->{
            result.completeExceptionally(new PulsarClientException(ex.getMessage()));
            return null;
        });
        return result;
    }

}
