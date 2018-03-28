package org.apache.pulsar.replicator.kinesis.function;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import jersey.repackaged.com.google.common.collect.Maps;

public class KinesisReplicatorFunction implements Function<String, Boolean> {

	private static final String REPLICATOR_CLASS_NAME = "org.apache.pulsar.replicator.api.kinesis.KinesisProducerManager";
	private static final String jarPath = "/yh/git/july/pulsar/pulsar-replicator/pulsar-replicator-kinesis/pulsar-replicator-kinesis-core/target/pulsar-replicator-kinesis-core.jar";
	private final static Map<String, Object> replicatorMap = Maps.newConcurrentMap();

	@Override
	public Boolean process(String action, Context context) throws Exception {
		final String replProcessId = replicatorProcessId(context);
		Object kinesisReplicatorManager = replicatorMap.get(replProcessId);
		switch (action) {
		case "stop":
			if (kinesisReplicatorManager != null) {
				close(kinesisReplicatorManager);
			}
			break;
		case "restart":
			if (kinesisReplicatorManager != null) {
				close(kinesisReplicatorManager);
			}
			kinesisReplicatorManager = null;
		case "start":
			if (kinesisReplicatorManager == null) {
				startReplicator(context);
			}
			break;
		default:
			// TODO: log invalid action
			return false;
		}
		return true;
	}

	@Override
	public void init(Context context) throws Exception {
		startReplicator(context);
	}

	public void startReplicator(Context context) throws Exception {
		final String replProcessId = replicatorProcessId(context);
		String brokerServiceUrl = context.getUserConfigValue("brokerServiceUrl");
		String zkServerUrl = context.getUserConfigValue("zkServerUrl");
		String replTopicName = context.getUserConfigValue("replTopicName");
		context.getLogger().info("starting replicator {}-{}", replProcessId, replTopicName);

		File jarFile = new File(jarPath);
		URL[] urls = { jarFile.toURL() };
		URLClassLoader classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
		Object replicatorInstance = Class.forName(REPLICATOR_CLASS_NAME, true, classLoader)
				.getConstructor(String.class, String.class, String.class)
				.newInstance(replTopicName, brokerServiceUrl, zkServerUrl);
		context.getLogger().info("Successfully started replicator {}", replProcessId);
		replicatorMap.put(replProcessId, replicatorInstance);
	}

	private String replicatorProcessId(Context context) {
		return context != null ? String.format("%s-%s", context.getInstanceId(), context.getFunctionId()) : null;
	}

	private void close(Object kinesisReplicatorManager) {
		try {
			Method method = Class.forName(REPLICATOR_CLASS_NAME, true, this.getClass().getClassLoader())
					.getDeclaredMethod("close");
			method.invoke(kinesisReplicatorManager);
		} catch (Exception e) {
			// TODO: failed to close replicator
		}
	}

}
