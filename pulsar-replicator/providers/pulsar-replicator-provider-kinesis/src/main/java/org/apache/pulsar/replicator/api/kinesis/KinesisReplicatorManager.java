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

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.api.AbstractReplicatorManager;

/**
 * Kinesis Replicator Manager that starts Kinesis producer and start reading
 * message from pulsar and replicate to targeted replication system
 *
 */
public class KinesisReplicatorManager extends AbstractReplicatorManager {

	@Override
	public void startProducer(String topicName, ReplicatorPolicies replicatorPolicies) {
		(new KinesisReplicatorProvider()).createProducerAsync(topicName, replicatorPolicies).thenAccept(producer -> {
			this.producer = producer;
			// producer started successfully.. trigger reading message
			readMessage();
		}).exceptionally(ex -> {
			pulsarClient.timer().newTimeout(timeout -> {
				startProducer(topicName, replicatorPolicies);
			}, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
			return null;
		});
	}

	@Override
	public ReplicatorType getType() {
		return ReplicatorType.Kinesis;
	}

	@Override
	protected void stopProducer() throws Exception {
		if (this.producer != null) {
			this.producer.close();
		}
	}

}
