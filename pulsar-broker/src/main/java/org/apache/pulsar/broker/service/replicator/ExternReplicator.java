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
package org.apache.pulsar.broker.service.replicator;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.web.PulsarWebResource.path;
import static org.apache.pulsar.replicator.kinesis.function.ReplicatorFunction.CONF_REPLICATOR_CLUSTER_VAL;
import static org.apache.pulsar.replicator.kinesis.function.ReplicatorFunction.CONF_REPLICATOR_NAMESPACE_VAL;
import static org.apache.pulsar.replicator.kinesis.function.ReplicatorFunction.CONF_REPLICATOR_TENANT_VAL;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig.Runtime;
import org.apache.pulsar.functions.proto.Function.FunctionConfig.SubscriptionType;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.replicator.api.kinesis.KinesisReplicatorManager;
import org.apache.pulsar.replicator.kinesis.function.ReplicatorFunction;
import org.apache.pulsar.replicator.kinesis.function.ReplicatorTopicData;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest.Action;
import org.apache.pulsar.replicator.kinesis.function.utils.ReplicatorTopicDataSerDe;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition.FormDataContentDispositionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import static org.apache.pulsar.broker.admin.AdminResource.jsonMapper;

public class ExternReplicator {

	public static Response internalRegisterReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
			ReplicatorType replicatorType) {
		// TODO: validate if topic-name present under topic-name mapping
		InputStream inputStream = null;
		File file = null;
		try {
			String replicatorFilePath = ReplicatorFunction.class.getProtectionDomain().getCodeSource().getLocation()
					.getPath();
			inputStream = new FileInputStream(replicatorFilePath);
			file = new File(replicatorFilePath);
		} catch (RestException e) {
			throw e;
		} catch (Exception e) {
			log.warn("Replicator function jar not found in classpath", e);
			throw new RestException(Status.NOT_FOUND, "Replicator function jar not found in classpath");
		}
		FormDataContentDispositionBuilder builder = FormDataContentDisposition.name(file.getName());
		FormDataContentDisposition fileDetail = builder.fileName(file.getName())
				.creationDate(new Date(file.lastModified())).build();
		return internalRegisterReplicatorOnTopic(pulsar, topicName, replicatorType, inputStream, fileDetail);
	}

	public static void internalUpdateReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
			ReplicatorType replicatorType, Action action) {
		try {
			internalUpdateReplicatorOnTopicAsync(pulsar, topicName, replicatorType, action).get();
		} catch (Exception e) {
			log.error("Failed to update replicator function for topic {} {}-{}", topicName, replicatorType, action, e);
			throw new RestException(e);
		}
	}

	public static void internalDeRegisterReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
			ReplicatorType replicatorType) {
		String functionName = formFunctionName(replicatorType, topicName);
		FunctionsImpl.deregisterFunction(CONF_REPLICATOR_TENANT_VAL, CONF_REPLICATOR_NAMESPACE_VAL, functionName,
				pulsar.getWorkerService());
	}

	private static Response internalRegisterReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
			ReplicatorType replicatorType, InputStream uploadedInputStream, FormDataContentDisposition fileDetail) {
		String functionName = formFunctionName(replicatorType, topicName);
		String className = ReplicatorFunction.class.getName();
		FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
		String replicatorTopic = ReplicatorFunction.getFunctionTopicName(replicatorType);
		String replicatorTopicSerClassName = ReplicatorTopicDataSerDe.class.getName();
		Map<String, String> userConfigs = Maps.newHashMap();
		userConfigs.put(ReplicatorFunction.CONF_BROKER_SERVICE_URL, pulsar.getBrokerServiceUrl());
		userConfigs.put(ReplicatorFunction.CONF_ZK_SERVER_URL, pulsar.getConfiguration().getZookeeperServers());
		userConfigs.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, topicName.toString());
		String replicatorManagerClassName = KinesisReplicatorManager.class.getName(); // TODO: get class-name based on
																						// replicatorType
		userConfigs.put(ReplicatorFunction.CONF_REPLICATOR_MANAGER_CLASS_NAME, replicatorManagerClassName);
		functionConfigBuilder.setTenant(CONF_REPLICATOR_TENANT_VAL).setNamespace(CONF_REPLICATOR_NAMESPACE_VAL)
				.setName(functionName).setClassName(className).setParallelism(1).setRuntime(Runtime.JAVA)
				.setAutoAck(true)
				.putCustomSerdeInputs(replicatorTopic, replicatorTopicSerClassName).putAllUserConfig(userConfigs);
		FunctionConfig functionConfig = functionConfigBuilder.build();

		createNamespaceIfNotCreated(pulsar, TopicName.get(replicatorTopic));

		return FunctionsImpl.registerFunction(CONF_REPLICATOR_TENANT_VAL, CONF_REPLICATOR_NAMESPACE_VAL, functionName,
				uploadedInputStream, fileDetail, functionConfig, pulsar.getWorkerService());
	}

	private static CompletableFuture<Void> internalUpdateReplicatorOnTopicAsync(PulsarService pulsar,
			TopicName topicName, ReplicatorType replicatorType, Action action) {
		CompletableFuture<Void> result = new CompletableFuture<>();
		final String clusterName = pulsar.getConfiguration().getClusterName();
		PulsarClient client = pulsar.getBrokerService().getReplicationClient(clusterName);
		if (client == null) {
			throw new RestException(Status.NOT_FOUND, "Couldn't initialize client for cluster " + clusterName);
		}
		ProducerBuilder<byte[]> producerBuilder = client.newProducer() //
				.topic(ReplicatorFunction.getFunctionTopicName(replicatorType)).sendTimeout(0, TimeUnit.SECONDS) //
				.maxPendingMessages(10) //
				.producerName(String.format("%s-%s", replicatorType.toString(), clusterName));
		ReplicatorTopicData topicActionData = new ReplicatorTopicData();
		topicActionData.action = action;
		topicActionData.topicName = topicName.toString();
		byte[] data = ReplicatorTopicDataSerDe.instance().serialize(topicActionData);
		producerBuilder.createAsync().thenAccept(producer -> {
			producer.sendAsync(data).thenAccept(res -> {
				pulsar.getExecutor().submit(() -> producer.closeAsync());
				result.complete(null);
			}).exceptionally(e -> {
				pulsar.getExecutor().submit(() -> producer.closeAsync());
				result.completeExceptionally(e);
				return null;
			});
		}).exceptionally(ex -> {
			result.completeExceptionally(ex);
			return null;
		});
		return result;
	}

	// TODO: remove cluster
	private static void createNamespaceIfNotCreated(PulsarService pulsar, TopicName topic) {
		try {
			String propertyPath = path(POLICIES, topic.getProperty());
			if (!pulsar.getConfigurationCache().propertiesCache().get(propertyPath).isPresent()) {
				PropertyAdmin propertyAdmin = new PropertyAdmin();
				propertyAdmin.setAdminRoles(Lists.newArrayList(pulsar.getConfiguration().getSuperUserRoles()));
				Set<String> clusters = pulsar.getConfigurationCache().clustersListCache().get();
				clusters.remove("global");
				propertyAdmin.setAllowedClusters(clusters);
				zkCreateOptimistic(pulsar, propertyPath, jsonMapper().writeValueAsBytes(propertyAdmin));
			}
		} catch (Exception e) {
			log.warn("Failed to create property {} ", topic.getProperty(), e);
			throw new RestException(e);
		}
		try {
			String namespacePath = path(POLICIES, topic.getNamespaceObject().toString());
			if (!pulsar.getConfigurationCache().policiesCache().get(namespacePath).isPresent()) {
				Policies policies = new Policies();
				Set<String> clusters = pulsar.getConfigurationCache().clustersListCache().get();
				clusters.remove("global");
				policies.replication_clusters = Lists.newArrayList(clusters);
				zkCreateOptimistic(pulsar, namespacePath, jsonMapper().writeValueAsBytes(policies));
			}
		} catch (Exception e) {
			log.warn("Failed to create namespace policies {}/{} ", topic.getProperty(), topic.getNamespace(), e);
			throw new RestException(e);
		}
	}

	private static String formFunctionName(ReplicatorType replicatorType, TopicName topicName) {
		return String.format("%s-%s-%s-%s", replicatorType.toString(), topicName.getProperty(), topicName.getNamespacePortion(), topicName.getLocalName());
	}
	
	private static void zkCreateOptimistic(PulsarService pulsar, String path, byte[] content)
			throws KeeperException, InterruptedException {
		ZkUtils.createFullPathOptimistic(pulsar.getGlobalZkCache().getZooKeeper(), path, content,
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	private static final Logger log = LoggerFactory.getLogger(ExternReplicator.class);
}
