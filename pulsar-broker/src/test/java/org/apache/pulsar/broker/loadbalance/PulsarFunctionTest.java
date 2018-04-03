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
package org.apache.pulsar.broker.loadbalance;

import static org.mockito.Mockito.spy;

import java.io.File;
import java.net.InetAddress;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminWithFunctions;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig.Builder;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.replicator.api.kinesis.KinesisReplicatorManager;
import org.apache.pulsar.replicator.auth.DefaultAuthorizationKeyStore;
import org.apache.pulsar.replicator.kinesis.function.ReplicatorFunction;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 */
public class PulsarFunctionTest {
	LocalBookkeeperEnsemble bkEnsemble;

	ServiceConfiguration config;
	URL url;
	PulsarService pulsar;
	PulsarAdmin admin;
	PulsarClient pulsarClient;
	PulsarAdminWithFunctions funAdmin;
	BrokerStats brokerStatsClient;
	final String property = "pulsar-pro11";
	final String function = "fun4";
	String pulsarFunctionsNamespace = property + "/use/pulsar-function-admin";

	final String inputTopic = "persistent://" + property + "/use/ns/input";
	final String outputTopic = "persistent://" + property + "/use/ns/output";

	String primaryHost;

	ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

	private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
	private final int PRIMARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
	private final int PRIMARY_BROKER_PORT = PortManager.nextFreePort();
	private static final Logger log = LoggerFactory.getLogger(PulsarFunctionTest.class);

	static {
		System.setProperty("test.basePort", "16100");
	}

	@BeforeMethod
	void setup() throws Exception {

		// Start local bookkeeper ensemble
		bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, PortManager.nextFreePort());
		bkEnsemble.start();

		// Start broker 1
		config = spy(new ServiceConfiguration());
		config.setClusterName("use");
		config.setWebServicePort(PRIMARY_BROKER_WEBSERVICE_PORT);
		config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
		// config1.setZookeeperServers("127.0.0.1" + ":" + 2181);
		config.setBrokerServicePort(PRIMARY_BROKER_PORT);
		config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
		WorkerService functionsWorkerService = createPulsarFunctionWorker(config);
		pulsar = new PulsarService(config, Optional.of(functionsWorkerService));

		pulsar.start();

		url = new URL("http://127.0.0.1" + ":" + PRIMARY_BROKER_WEBSERVICE_PORT);
		admin = new PulsarAdmin(url, (Authentication) null);
		funAdmin = new PulsarAdminWithFunctions(url, new ClientConfigurationData());
		brokerStatsClient = admin.brokerStats();
		primaryHost = String.format("http://%s:%d", InetAddress.getLocalHost().getHostName(),
				PRIMARY_BROKER_WEBSERVICE_PORT);

		pulsarClient = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();

		PropertyAdmin propAdmin = new PropertyAdmin();
		propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
		admin.properties().createProperty(property, propAdmin);
		functionsWorkerService.start();

		Thread.sleep(100);
	}

	private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {
		WorkerConfig workerConfig;
		workerConfig = new WorkerConfig();
		workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
		workerConfig.setSchedulerClassName(
				org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
		workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
		// worker talks to local broker
		workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort());
		workerConfig.setPulsarWebServiceUrl("http://127.0.0.1:" + config.getWebServicePort());
		workerConfig.setFailureCheckFreqMs(100);
		workerConfig.setNumFunctionPackageReplicas(1);
		workerConfig.setClusterCoordinationTopicName("coordinate");
		workerConfig.setFunctionAssignmentTopicName("assignment");
		workerConfig.setFunctionMetadataTopicName("metadata");
		workerConfig.setInstanceLivenessCheckFreqMs(100);
		String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
		workerConfig.setWorkerHostname(hostname);
		workerConfig
				.setWorkerId("c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort());
		return new WorkerService(workerConfig);
	}

	@AfterMethod
	void shutdown() throws Exception {
		log.info("--- Shutting down ---");
		executor.shutdown();

		admin.close();

		pulsar.close();

		bkEnsemble.stop();
	}

	@Test(enabled = true)
	public void testPulsarFunctionE2E() throws Exception {

		final String tenant = "prop";
		final String namespace = "ns";
		final String inputSerdeClassName = DefaultSerDe.class.getName();
		final String outputSerdeClassName = DefaultSerDe.class.getName();
		final String testFile = "/yh/temp/pulsar/apache-pulsar-2.0.0-incubating-functions-preview/examples/api-examples.jar";
		final String className = "org.apache.pulsar.functions.api.examples.JavaNativeExclmationFunction";

		FunctionConfig functionConfig = FunctionConfig.newBuilder().setTenant(tenant).setNamespace(namespace)
				.setName(function).setOutput(outputTopic).putCustomSerdeInputs(inputTopic, inputSerdeClassName)
				.setOutputSerdeClassName(outputSerdeClassName).setClassName(className).setParallelism(1).build();

		funAdmin.functions().createFunction(functionConfig, testFile);
		System.out.println("Finish");

		Thread.sleep(3000);

		Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(outputTopic).subscriptionName("my-subscriber-name")
				.subscribe();

		ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(inputTopic);

		Producer<byte[]> producer = producerBuilder.create();
		List<Future<MessageId>> futures = Lists.newArrayList();

		// Asynchronously produce messages
		for (int i = 0; i < 10; i++) {
			final String message = "my-message-" + i;
			Future<MessageId> future = producer.sendAsync(message.getBytes());
			futures.add(future);
		}

		log.info("Waiting for async publish to complete");
		for (Future<MessageId> future : futures) {
			future.get();
		}

		Message<byte[]> msg = null;
		Set<String> messageSet = Sets.newHashSet();
		for (int i = 0; i < 10; i++) {
			msg = consumer.receive(5, TimeUnit.SECONDS);
			String receivedMessage = new String(msg.getData());
			log.info("Received message: [{}]", receivedMessage);
			String expectedMessage = "my-message-" + i;
			System.out.println(receivedMessage);
		}

	}

	
	@Test(enabled = true)
	public void testPulsarFunctionE2EKinesis() throws Exception {

		final String tenant = "prop";
		final String namespace = "ns";
		final String inputSerdeClassName = DefaultSerDe.class.getName();
		final String outputSerdeClassName = DefaultSerDe.class.getName();
		final String testFile = "/yh/git/july/pulsar/pulsar-replicator/function/target/pulsar-replicator-function.jar";
		//final String testFile = "/yh/temp/function/kinesis.jar";
		final String className = ReplicatorFunction.class.getName();

		
		final String replNamespace = property + "/global/replicator";
		final String topicName = "replTopic";
		final String replTopicName = "persistent://" + replNamespace +"/"+topicName;
		admin.namespaces().createNamespace(replNamespace);
		admin.namespaces().setNamespaceReplicationClusters(replNamespace, Lists.newArrayList("use"));
		ReplicatorPolicies replicatorPolicies = new ReplicatorPolicies();
		Map<String, String> replicationProperties = Maps.newHashMap();
		Map<String, String> topicMapping = Maps.newHashMap();
		topicMapping.put(topicName, "KineisFunction:us-west-2");
		replicationProperties.put("accessKey", "AK");
		replicationProperties.put("secretKey", "SK");
		replicatorPolicies.topicNameMapping = topicMapping;
		replicatorPolicies.replicationProperties = replicationProperties;
		replicatorPolicies.authPluginName = DefaultAuthorizationKeyStore.class.getName();
		admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, replicatorPolicies);
		
		
		Map<String,String> userConfig = Maps.newHashMap();
		final File kinesisReplicatorFile = new File("../pulsar-replicator/providers/pulsar-replicator-provider-kinesis/target/pulsar-replicator-provider-kinesis.jar");
		userConfig.put(ReplicatorFunction.CONF_BROKER_SERVICE_URL, url.toString());
		userConfig.put(ReplicatorFunction.CONF_ZK_SERVER_URL, config.getZookeeperServers());
		userConfig.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, replTopicName);
		userConfig.put(ReplicatorFunction.CONF_REPLICATOR_JAR_NAME, kinesisReplicatorFile.getAbsolutePath());
		userConfig.put(ReplicatorFunction.CONF_REPLICATOR_MANAGER_CLASS_NAME, KinesisReplicatorManager.class.getName());
		
		Builder functionConfigBuilder = FunctionConfig.newBuilder();
		functionConfigBuilder.putAllUserConfig(userConfig);
		FunctionConfig functionConfig = functionConfigBuilder.setTenant(tenant).setNamespace(namespace)
				.setName(function).setOutput(outputTopic).putCustomSerdeInputs(inputTopic, inputSerdeClassName)
				.setOutputSerdeClassName(outputSerdeClassName).setClassName(className).setParallelism(1).build();

		funAdmin.functions().createFunction(functionConfig, testFile);
		System.out.println("Finish function uploading");

		Thread.sleep(3000);

		
		// trigger function
		Producer<byte[]> kinesisFunctionTopic = pulsarClient.newProducer().topic(inputTopic).create();
		kinesisFunctionTopic.send("start".getBytes());
		
		Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(outputTopic).subscriptionName("my-subscriber-name")
				.subscribe();

		ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(replTopicName);

		Producer<byte[]> producer = producerBuilder.create();
		List<Future<MessageId>> futures = Lists.newArrayList();

		// Asynchronously produce messages
		for (int i = 0; i < 10; i++) {
			final String message = "my-message-" + i;
			Future<MessageId> future = producer.sendAsync(message.getBytes());
			futures.add(future);
		}

		log.info("Waiting for async publish to complete");
		for (Future<MessageId> future : futures) {
			future.get();
		}

		System.out.println("finish");

	}
	
	@Test
	public void testKinesisReplicatorFunction() throws Exception {
		final String replNamespace = property + "/global/replicator";
		final String topicName = "replTopic";
		final String replTopicName = "persistent://" + replNamespace +"/"+topicName;
		
		admin.namespaces().createNamespace(replNamespace);
		admin.namespaces().setNamespaceReplicationClusters(replNamespace, Lists.newArrayList("use"));
		ReplicatorPolicies replicatorPolicies = new ReplicatorPolicies();
		Map<String, String> topicMapping = Maps.newHashMap();
		Map<String, String> replicationProperties = Maps.newHashMap();
		topicMapping.put(topicName, "KineisFunction:us-west-2");
		replicationProperties.put("accessKey", "AK");
		replicationProperties.put("secretKey", "SK");
		replicatorPolicies.topicNameMapping = topicMapping;
		replicatorPolicies.replicationProperties = replicationProperties;
		replicatorPolicies.authPluginName = DefaultAuthorizationKeyStore.class.getName();
		admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, replicatorPolicies);
		
		ReplicatorFunction function = new ReplicatorFunction();
		Map<String,String> userConfig = Maps.newHashMap();
		final File kinesisReplicatorFile = new File("../pulsar-replicator/providers/pulsar-replicator-provider-kinesis/target/pulsar-replicator-provider-kinesis.jar");
		userConfig.put(ReplicatorFunction.CONF_BROKER_SERVICE_URL, url.toString());
		userConfig.put(ReplicatorFunction.CONF_ZK_SERVER_URL, config.getZookeeperServers());
		userConfig.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, replTopicName);
		userConfig.put(ReplicatorFunction.CONF_REPLICATOR_JAR_NAME, kinesisReplicatorFile.getAbsolutePath());
		userConfig.put(ReplicatorFunction.CONF_REPLICATOR_MANAGER_CLASS_NAME, KinesisReplicatorManager.class.getName());
		Context context = new ContextImpl(userConfig);
		function.process(ReplicatorFunction.Action.Start.toString(), context);
		
		ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(replTopicName);

		Producer<byte[]> producer = producerBuilder.create();
		List<Future<MessageId>> futures = Lists.newArrayList();

		// Asynchronously produce messages
		for (int i = 0; i < 10; i++) {
			final String message = "my-message-" + i;
			Future<MessageId> future = producer.sendAsync(message.getBytes());
			futures.add(future);
		}

		log.info("Waiting for async publish to complete");
		for (Future<MessageId> future : futures) {
			future.get();
		}
		
		System.out.println("finish");
	}

	class ContextImpl implements Context {

		Map<String,String> userConfig;
		
		public ContextImpl(Map<String,String> userConfig) {
			this.userConfig = userConfig;
		}
		
		@Override
		public CompletableFuture<Void> ack(byte[] arg0, String arg1) {
			return null;
		}

		@Override
		public String getFunctionId() {
			return null;
		}

		@Override
		public String getFunctionName() {
			return null;
		}

		@Override
		public String getFunctionVersion() {
			return null;
		}

		@Override
		public Collection<String> getInputTopics() {
			return null;
		}

		@Override
		public String getInstanceId() {
			return null;
		}

		@Override
		public Logger getLogger() {
			return null;
		}

		@Override
		public byte[] getMessageId() {
			return null;
		}

		@Override
		public String getNamespace() {
			return null;
		}

		@Override
		public String getOutputSerdeClassName() {
			return null;
		}

		@Override
		public String getOutputTopic() {
			return null;
		}

		@Override
		public String getTenant() {
			return null;
		}

		@Override
		public String getTopicName() {
			return null;
		}

		@Override
		public String getUserConfigValue(String config) {
			return this.userConfig.get(config);
		}

		@Override
		public void incrCounter(String arg0, long arg1) {
		}

		@Override
		public <O> CompletableFuture<Void> publish(String arg0, O arg1) {
			return null;
		}

		@Override
		public <O> CompletableFuture<Void> publish(String arg0, O arg1, String arg2) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void recordMetric(String arg0, double arg1) {
		}
		
	}
	
}
