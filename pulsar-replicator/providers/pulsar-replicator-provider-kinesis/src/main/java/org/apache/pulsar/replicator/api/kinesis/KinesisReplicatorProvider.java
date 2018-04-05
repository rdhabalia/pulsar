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
package org.apache.pulsar.replicator.api.kinesis;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.apache.pulsar.replicator.api.ReplicatorProvider;
import org.apache.pulsar.replicator.auth.AuthParamKeyStore;
import org.apache.pulsar.replicator.auth.AuthParamKeyStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Kinesis replicator provider that validates replicator
 * configuration/properties and creates replication producer.
 *
 */
public class KinesisReplicatorProvider implements ReplicatorProvider {

	public static final String ACCESS_KEY_NAME = "accessKey";
	public static final String CREDENTIAL_KEY_NAME = "secretKey";

	private static final KinesisReplicatorProvider instance = new KinesisReplicatorProvider();

	public static KinesisReplicatorProvider instance() {
		return instance;
	}

	@Override
	public ReplicatorType getType() {
		return ReplicatorType.Kinesis;
	}

	@Override
	public void validateProperties(String namespace, ReplicatorPolicies replicatorPolicies, Map<String, String> authData)
			throws IllegalArgumentException {
		Map<String, String> topicProperties = replicatorPolicies.topicNameMapping;
		try {
			if (authData == null || !authData.containsKey(ACCESS_KEY_NAME)
					|| !authData.containsKey(CREDENTIAL_KEY_NAME)) {
				throw new IllegalArgumentException(
						String.format("Auth data requires %s and %s to authenticate to kinesis stream", ACCESS_KEY_NAME,
								CREDENTIAL_KEY_NAME));
			}
		} catch (IllegalArgumentException e) {
			log.error("Failed to validate auth data for {}, {}", namespace, e.getMessage());
		} catch (Exception e) {
			log.error("Failed to fetch auth data for {}", namespace, e);
			throw new IllegalArgumentException(e);
		}

		if (topicProperties != null) {
			for (Entry<String, String> topicEntry : topicProperties.entrySet()) {
				String streamName = topicEntry.getValue();
				if (StringUtils.isBlank(streamName) || !streamName.contains(":")) {
					throw new IllegalArgumentException(String.format(
							"Invalid stream name %s Kinesis stream must be <streamName>:<regionName> for topic %s",
							streamName, topicEntry.getKey()));
				}
			}
		}

	}

	@Override
	public CompletableFuture<ReplicatorProducer> createProducerAsync(String topicName,
			ReplicatorPolicies replicatorPolicies) {

		if (replicatorPolicies == null || replicatorPolicies.topicNameMapping == null) {
			throw new IllegalStateException("Topic-mapping of ReplicatorPolicies can't be empty");
		}
		String streamParam = replicatorPolicies.topicNameMapping.get(TopicName.get(topicName).getLocalName());
		if (StringUtils.isBlank(streamParam) || !streamParam.contains(":")) {
			throw new IllegalStateException("invalid stream param [streamName:regionName] " + streamParam);
		}
		int splitIndex = streamParam.lastIndexOf(":");
		String streamName = streamParam.substring(0, splitIndex);
		String regionName = streamParam.substring(splitIndex + 1);
		Region region = Region.getRegion(Regions.fromName(regionName));
		try {
			AWSCredentials credentials = fetchCredential(topicName, replicatorPolicies);
			return CompletableFuture
					.completedFuture(new KinesisReplicatorProducer(topicName, streamName, region, credentials));
		} catch (Exception e) {
			log.error("Failed to fetch auth data for {}", topicName, e);
			return FutureUtil.failedFuture(e);
		}
	}

	private AWSCredentials fetchCredential(String topicName, ReplicatorPolicies replicatorPolicies) throws Exception {
		Map<String, String> authDataMap = getAuthData(TopicName.get(topicName).getNamespaceObject().toString(),
				replicatorPolicies);
		return new AWSCredentials() {
			@Override
			public String getAWSAccessKeyId() {
				return authDataMap.get(ACCESS_KEY_NAME);
			}

			@Override
			public String getAWSSecretKey() {
				return authDataMap.get(CREDENTIAL_KEY_NAME);
			}
		};
	}

	private Map<String, String> getAuthData(String namespace, ReplicatorPolicies replicatorPolicies) throws Exception {
		String pluginName = replicatorPolicies.authParamStorePluginName;
		AuthParamKeyStore authKeyStore = AuthParamKeyStoreFactory.create(pluginName);
		return authKeyStore.fetchAuthData(namespace, replicatorPolicies.replicationProperties);
	}

	private static final Logger log = LoggerFactory.getLogger(KinesisReplicatorProvider.class);

}
