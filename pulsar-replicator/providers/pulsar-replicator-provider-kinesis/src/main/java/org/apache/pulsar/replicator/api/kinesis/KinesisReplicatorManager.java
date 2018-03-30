package org.apache.pulsar.replicator.api.kinesis;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.api.AbstractReplicatorManager;

public class KinesisReplicatorManager extends AbstractReplicatorManager {

	@Override
	public void startProducer(String topicName, ReplicatorPolicies replicatorPolicies) {
		(new KinesisReplicatorProvider()).createProducerAsync(topicName, replicatorPolicies).thenAccept(producer -> {
			this.producer = producer;
			readMessage();
		}).exceptionally(ex -> {
			pulsarClient.timer().newTimeout(timeout -> {
				startProducer(topicName, replicatorPolicies);
			}, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
			return null;
		});
	}

}
