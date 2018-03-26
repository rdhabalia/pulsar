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
package org.apache.pulsar.replicator.api;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;

public interface ReplicatorProvider {
	/**
	 * Returns Enum-type of replicator-provider so, replicator-service can map
	 * provider with its type
	 * 
	 * @return Enum-type of provider eg: Kinesis, JMS, KAFKA
	 */
	public ReplicatorType getType();

	/**
	 * Validates topic-properties and credential-properties which must be compliance
	 * with This method will be called by broker to validate
	 * replicator-configuration while onboarding replication configuration.
	 * 
	 * @throws IllegalArgumentException
	 */
	public void validateProperties(Map<String, String> topicProperties, Map<String, String> credentialProperties)
			throws IllegalArgumentException;

	/**
	 * Create Replicator-producer async which will publish messages to targeted
	 * replication-system.
	 */
	public CompletableFuture<ReplicatorProducer> createProducerAsync(final String topic,
			final ReplicatorPolicies replicatorPolicies);

}
