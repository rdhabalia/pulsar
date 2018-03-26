package org.apache.pulsar.replicator.api.kinesis;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.apache.pulsar.replicator.api.ReplicatorProvider;

public class KinesisReplicatorProvider implements ReplicatorProvider{

	@Override
	public ReplicatorType getType() {
		return ReplicatorType.Kinesis;
	}

	@Override
	public void validateProperties(Map<String, String> topicProperties, Map<String, String> credentialProperties)
			throws IllegalArgumentException {
		// TODO validate credential by calling ykeykey
	}

	@Override
	public CompletableFuture<ReplicatorProducer> createProducerAsync(String topic,
			ReplicatorPolicies replicatorPolicies) {
		return CompletableFuture.completedFuture(new KinesisReplicatorProducer());
	}

}
