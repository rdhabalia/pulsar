package org.apache.kafka.clients.consumer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.kafka.compat.PulsarClientKafkaConfig;
import org.apache.pulsar.common.util.FutureUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.TopicFilter;
import kafka.serializer.Decoder;
import org.apache.pulsar.shade.io.netty.util.concurrent.DefaultThreadFactory;


/**
 * Not implementing kafka.javaapi.consumer.ConsumerConnector because its method has KafkaStream api signature and
 * KafkaStream is a scala class which creates unresolvable dependency conflict 
 * src:
 * https://github.com/apache/kafka/blob/0.8.2.2/core/src/main/scala/kafka/javaapi/consumer/ConsumerConnector.java
 */
public class ConsumerConnector { // implements kafka.javaapi.consumer.ConsumerConnector {

    private final PulsarClient client;
    private final boolean isAutoCommit;
    private final ConsumerBuilder<byte[]> consumerBuilder;
    private String clientId;
    @SuppressWarnings("rawtypes")
    private final Set<PulsarKafkaStream> topicStreams;
    private SubscriptionInitialPosition strategy = null;
    private final ScheduledExecutorService executor;

    public ConsumerConnector(ConsumerConfig config) {
        checkNotNull(config, "ConsumerConfig can't be null");
        int autoCommitIntervalMs = config.autoCommitIntervalMs();
        this.clientId = config.clientId();
        String consumerId = !config.consumerId().isEmpty() ? config.consumerId().get() : null;
        long consumerTimeOutMs = config.consumerTimeoutMs();
        String groupId = config.groupId();
        int maxMessage = config.queuedMaxMessages();
        this.isAutoCommit = config.autoCommitEnable();
        if ("largest".equalsIgnoreCase(config.autoOffsetReset())) {
            strategy = SubscriptionInitialPosition.Latest;
        } else if ("smallest".equalsIgnoreCase(config.autoOffsetReset())) {
            strategy = SubscriptionInitialPosition.Earliest;
        }
        String serviceUrl = config.zkConnect();

        Properties properties = config.props() != null && config.props().props() != null ? config.props().props()
                : new Properties();
        try {
            client = PulsarClientKafkaConfig.getClientBuilder(properties).serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new IllegalArgumentException(
                    "Failed to create pulsar-client using url = " + serviceUrl + ", properties = " + properties, e);
        }

        topicStreams = Sets.newConcurrentHashSet();
        consumerBuilder = client.newConsumer().subscriptionName(groupId).receiverQueueSize(maxMessage);
        if (consumerId != null) {
            consumerBuilder.consumerName(consumerId);
        }
        if (autoCommitIntervalMs > 0) {
            consumerBuilder.acknowledgmentGroupTime(autoCommitIntervalMs, TimeUnit.MILLISECONDS);
        }
        
        this.executor = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-kafka"));
    }

    public <K, V> Map<String, List<PulsarKafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) {
        return createMessageStreamsByFilter(null, topicCountMap, null, null);
    }
    
    public <K, V> Map<String, List<PulsarKafkaStream<K, V>>> createMessageStreams(Map<String, Integer> topicCountMap,
            Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        return createMessageStreamsByFilter(null, topicCountMap, keyDecoder, valueDecoder);
    }
    
    public <K, V> Map<String, List<PulsarKafkaStream<K, V>>> createMessageStreamsByFilter(TopicFilter topicFilter, Map<String, Integer> topicCountMap,
            Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        
        Map<String, List<PulsarKafkaStream<K, V>>> streams = Maps.newHashMap();

        topicCountMap.forEach((topic, count) -> {
            try {
                Consumer<byte[]> consumer = consumerBuilder.topic(topic).subscribe();
                PulsarKafkaStream<K, V> stream = new PulsarKafkaStream<>(keyDecoder, valueDecoder, consumer,
                        isAutoCommit, clientId, strategy);
                //TODO: check why list?
                streams.put(topic, Collections.singletonList(stream));
                topicStreams.add(stream);
                
            } catch (PulsarClientException e) {
                // TODO: log exception
                throw new RuntimeException("Failed to subscribe on topic " + topic, e);
            }
        });
        return streams;
    }

    public List<PulsarKafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter) {
        throw new UnsupportedOperationException("method not supported");
    }

    public List<PulsarKafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter arg0, int arg1) {
        throw new UnsupportedOperationException("method not supported");
    }

    public <K, V> List<PulsarKafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int arg1, Decoder<K> arg2,
            Decoder<V> arg3) {
        throw new UnsupportedOperationException("method not supported");
    }
    
    public List<CompletableFuture<Void>> commitOffsetsAsync() {
        return topicStreams.stream().map(stream -> (CompletableFuture<Void>) stream.commitOffsets())
                .collect(Collectors.toList());
    }
    
    /**
     *  Commit the offsets of all broker partitions connected by this connector.
     */
    public void commitOffsets() {
        commitOffsetsAsync();
    }
    
    public void commitOffsets(boolean retryOnFailure) {
        FutureUtil.waitForAll(commitOffsetsAsync()).handle((res, ex) -> {
            if (ex != null) {
                // TODO: log debug
                if (retryOnFailure) {
                    this.executor.schedule(() -> commitOffsets(retryOnFailure), 30, TimeUnit.SECONDS);
                }
            }
            return null;
        });
    }

    /**
     *  Shut down the connector
     */
    public void shutdown() {
        if (executor != null) {
            executor.shutdown();
        }
    }
}
