package org.apache.pulsar.replicator.api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.zookeeper.ZooKeeperCache.cacheTimeOutInSec;

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
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.google.common.base.Joiner;

import io.netty.util.concurrent.DefaultThreadFactory;

public abstract class AbstractReplicatorManager
		implements ReplicatorManager, Function<Throwable, Void>, java.util.function.Consumer<Message> {

	protected ReplicatorProducer producer;
	protected PulsarClientImpl pulsarClient;
	protected String topicName;
	protected ReplicatorConfig config;

	private Consumer inputConsumer;
	private ZooKeeperClientFactory zkClientFactory = null;
	private ZooKeeperDataCache<Policies> policiesCache;
	private OrderedScheduler orderedExecutor;
	private ScheduledExecutorService cacheExecutor;
	private GlobalZooKeeperCache globalZkCache;

	// TODO: this should come from the user-config
	public static final String replPrefix = "pulsar";
	protected static final long READ_DELAY_BACKOFF_MS = 100;
	private static final int ZK_SESSION_TIME_OUT_MS = 30_000;
	private static final int MAX_ACK_RETRY = 10;

	// TODO:
	// protected String state;

	@Override
	public void start(ReplicatorConfig config) throws Exception {

		this.config = config;
		this.topicName = config.getTopicName();

		this.orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("pulsar-replicator-ordered")
				.build();
		this.cacheExecutor = Executors.newScheduledThreadPool(1,
				new DefaultThreadFactory("replicator-zk-cache-callback"));
		this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(), ZK_SESSION_TIME_OUT_MS,
				config.getZkServiceUrl(), orderedExecutor, this.cacheExecutor);
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

		pulsarClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(config.getBrokerServiceUrl())
				.statsInterval(0, TimeUnit.SECONDS).build();
		String subscriberName = String.format("%s.%s", replPrefix, config.getReplicatorType().toString());
		inputConsumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriberName).subscribe();
		ReplicatorPolicies replicatorPolicies = getReplicatorPolicies(topicName);
		try {
			startProducer(this.topicName, replicatorPolicies);
		} catch (Exception e) {
			// cleanup resources
			close();
			throw e;
		}

	}

	@Override
	public void close() throws Exception {
		if (this.inputConsumer != null) {
			this.inputConsumer.close();
		}
		if (this.producer != null) {
			this.producer.close();
		}
		if (this.pulsarClient != null) {
			this.pulsarClient.close();
		}
		if (this.globalZkCache != null) {
			this.globalZkCache.close();
		}
		if (this.orderedExecutor != null) {
			this.orderedExecutor.shutdown();
		}
		if (this.cacheExecutor != null) {
			this.cacheExecutor.shutdown();
		}
	}

	protected abstract void startProducer(String topicName, ReplicatorPolicies replicatorPolicies) throws Exception;

	@Override
	public void accept(Message message) {
		producer.send(message).thenAccept((res) -> {
			acknowledgeMessageWithRetry(message, 0);
			readMessage();
		}).exceptionally(ex -> {
			// TODO: log exception and retry with backoff
			System.out.println("failed  =" + new String(message.getData()) + "  and retry =" + READ_DELAY_BACKOFF_MS);
			pulsarClient.timer().newTimeout(timeout -> {
				accept(message);
			}, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
			return null;
		});
	}

	protected void readMessage() {
		inputConsumer.receiveAsync().thenAccept(this).exceptionally(this);
	}

	private void acknowledgeMessageWithRetry(Message message, int retry) {

		try {
			if (retry > MAX_ACK_RETRY) {
				return;
			}
			inputConsumer.acknowledge(message);
		} catch (PulsarClientException e) {
			// TODO: log
			pulsarClient.timer().newTimeout(timeout -> {
				acknowledgeMessageWithRetry(message, retry + 1);
			}, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
		}

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

	private ZooKeeperClientFactory getZooKeeperClientFactory() {
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
