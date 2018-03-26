package org.apache.pulsar.replicator.api.kinesis;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class KinesisReplicatorFunction implements Function<String, Boolean> {

	private KinesisProducerManager manager;

	@Override
	public Boolean process(String action, Context context) throws Exception {
		String brokerServiceUrl = context.getUserConfigValue("brokerServiceUrl");
		String zkServerUrl = context.getUserConfigValue("zkServerUrl");
		String replTopicName = context.getUserConfigValue("replTopicName");
		this.manager = new KinesisProducerManager(replTopicName, brokerServiceUrl, zkServerUrl);
		return true;
	}

}
