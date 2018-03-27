package org.apache.pulsar.replicator.api.kinesis;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.zookeeper.ZooKeeperCache.cacheTimeOutInSec;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.google.common.base.Joiner;

import io.netty.util.concurrent.DefaultThreadFactory;

public class KinesisProducerManager implements Function<Throwable, Void>, java.util.function.Consumer<Message> {

	private static final long READ_DELAY_BACKOFF_MS = 100;
	private static final int ZK_SESSION_TIME_OUT_MS = 30_000;
	private ReplicatorProducer producer;
	private Consumer inputConsumer;
	private PulsarClientImpl pulsarClient;
	private String topicName;

	private ZooKeeperClientFactory zkClientFactory = null;
	private ZooKeeperDataCache<Policies> policiesCache;
	private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(1)
			.name("pulsar-replicator-ordered").build();
	private final ScheduledExecutorService cacheExecutor = Executors.newScheduledThreadPool(1,
			new DefaultThreadFactory("replicator-zk-cache-callback"));

	public KinesisProducerManager(String topicName, String brokerServiceUrl, String zkServerUrl)
			throws IOException {

		this.topicName = topicName;

		GlobalZooKeeperCache globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
				ZK_SESSION_TIME_OUT_MS, zkServerUrl, orderedExecutor, this.cacheExecutor);
		globalZkCache.start();
		
		this.policiesCache = new ZooKeeperDataCache<Policies>(globalZkCache) {
			@Override
			public Policies deserialize(String path, byte[] content) throws Exception {
				return ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);
			}
		};
		this.policiesCache.registerListener((path, data, stat) -> {
			// TODO: restart kinesis producer if repl properties have been changed
		});

		pulsarClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(brokerServiceUrl)
				.statsInterval(0, TimeUnit.SECONDS).build();
		// TODO: change subscriber name
		inputConsumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name").subscribe();
		ReplicatorPolicies replicatorPolicies = getReplicatorPolicies(topicName);
		startProducer(this.topicName, replicatorPolicies);
	}

	private void startProducer(String topicName, ReplicatorPolicies replicatorPolicies) {
		(new KinesisReplicatorProvider()).createProducerAsync(topicName, replicatorPolicies)
				.thenAccept(producer -> {
					this.producer = producer;
					readMessage();
				}).exceptionally(ex -> {
					pulsarClient.timer().newTimeout(timeout -> {
						startProducer(topicName, replicatorPolicies);
					}, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
					return null;
				});
	}

	private void readMessage() {
		inputConsumer.receiveAsync().thenAccept(this).exceptionally(this);
	}

	@Override
	public void accept(Message message) {
		producer.send(message).thenAccept((res) -> readMessage()).exceptionally(ex -> {
			// TODO: log exception and retry with backoff
			pulsarClient.timer().newTimeout(timeout -> {
				accept(message);
			}, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
			return null;
		});
	}

	@Override
	public Void apply(Throwable t) {
		// TODO log and retry with backoff
		pulsarClient.timer().newTimeout(timeout -> readMessage(), READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
		return null;
	}

	private ReplicatorPolicies getReplicatorPolicies(String topicName) {
		final NamespaceName namespace = TopicName.get(this.topicName).getNamespaceObject();
		final String path = path(POLICIES, namespace.toString());
		try {
			Policies policies = this.policiesCache.getAsync(path).get(cacheTimeOutInSec, SECONDS)
					.orElseThrow(() -> new KeeperException.NoNodeException());
			if (policies.replicatorPolicies.containsKey(ReplicatorType.Kinesis)) {
				return policies.replicatorPolicies.get(ReplicatorType.Kinesis);
			} else {
				// TODO: throw exception
			}
		} catch (NoNodeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void close() throws PulsarClientException {
		producer.close();
		inputConsumer.close();
	}

	public ZooKeeperClientFactory getZooKeeperClientFactory() {
		if (zkClientFactory == null) {
			zkClientFactory = new ZookeeperBkClientFactoryImpl(orderedExecutor);
		}
		// Return default factory
		return zkClientFactory;
	}

	public static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }
	
}
