package org.apache.pulsar.replicator.kinesis.function;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.replicator.api.ReplicatorConfig;
import org.apache.pulsar.replicator.api.ReplicatorManager;

import jersey.repackaged.com.google.common.collect.Maps;

public class ReplicatorFunction implements Function<String, Boolean> {

	public static final String CONF_BROKER_SERVICE_URL = "brokerServiceUrl";
	public static final String CONF_ZK_SERVER_URL = "zkServerUrl";
	public static final String CONF_REPLICATION_TOPIC_NAME = "replTopicName";
	public static final String CONF_REPLICATOR_MANAGER_CLASS_NAME = "replManagerClassName";
	public static final String CONF_REPLICATOR_JAR_NAME = "replJar";
	public final static Map<String, ReplicatorManager> replicatorMap = Maps.newConcurrentMap();

	public enum Action {
		start, stop, restart;
	}

	// TODO: add state: if it's starting then ignore any process

	@Override
	public Boolean process(String action, Context context) throws Exception {
		System.out.println("process triggered " + context);
		final String replProcessId = replicatorProcessId(context);
		ReplicatorManager replicatorManager = replicatorMap.get(replProcessId);

		Action processAction = Action.valueOf(action);

		switch (processAction) {
		case stop:
			if (replicatorManager != null) {
				close(replicatorManager);
			}
			break;
		case restart:
			if (replicatorManager != null) {
				close(replicatorManager);
			}
			replicatorManager = null;
		case start:
			if (replicatorManager == null) {
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
		System.out.println("init triggered " + context);
		startReplicator(context);
	}

	/**
	 * Based on Replicator (eg: Kinesis, DynamoDB) it initialize the
	 * replicator-manager which internally manages replicator-producer.
	 * 
	 * @param context
	 * @throws Exception
	 */
	public void startReplicator(Context context) throws Exception {
		final String replProcessId = replicatorProcessId(context);
		String brokerServiceUrl = context.getUserConfigValue(CONF_BROKER_SERVICE_URL);
		String zkServerUrl = context.getUserConfigValue(CONF_ZK_SERVER_URL);
		String replTopicName = context.getUserConfigValue(CONF_REPLICATION_TOPIC_NAME);
		String jarPath = context.getUserConfigValue(CONF_REPLICATOR_JAR_NAME);
		String replManagerClassName = context.getUserConfigValue(CONF_REPLICATOR_MANAGER_CLASS_NAME);
		context.getLogger().info("starting replicator {}-{}", replProcessId, replTopicName);
		System.out.println("starting replicator " + replProcessId + ", " + replTopicName);

		System.out.println("loading replicator file " + jarPath);
		File jarFile = new File(jarPath);
		URL[] urls = { jarFile.toURL() };
		try (URLClassLoader classLoader = new URLClassLoader(urls, this.getClass().getClassLoader())) {
			ReplicatorManager replicatorManager = (ReplicatorManager) Class
					.forName(replManagerClassName, true, classLoader).getConstructor().newInstance();
			context.getLogger().info("Successfully started replicator manager {}", replProcessId);
			ReplicatorConfig replConfig = new ReplicatorConfig();
			replConfig.setTopicName(replTopicName);
			replConfig.setBrokerServiceUrl(brokerServiceUrl);
			replConfig.setZkServiceUrl(zkServerUrl);
			replConfig.setReplicatorType(ReplicatorType.Kinesis); // TODO: get replicator from the user-config
			replicatorManager.start(replConfig);
			replicatorMap.put(replProcessId, replicatorManager);
		} catch (Exception e) {
			System.out.println("Failed to load " + jarPath + ", " + e.getMessage());
			e.printStackTrace();
			throw e;
		}

	}

	private String replicatorProcessId(Context context) {
		return context != null ? String.format("%s-%s", context.getInstanceId(), context.getFunctionId()) : null;
	}

	private void close(ReplicatorManager replicatorManager) {
		try {
			System.out.println("*****  stopping replicator *******");
			replicatorManager.close();
		} catch (Exception e) {
			// TODO: failed to close replicator
		}
	}

}
