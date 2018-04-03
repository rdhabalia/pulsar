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
package org.apache.pulsar.replicator.kinesis.function;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.replicator.api.ReplicatorConfig;
import org.apache.pulsar.replicator.api.ReplicatorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;


public class ReplicatorFunction implements Function<String, Boolean> {

	public static final String CONF_BROKER_SERVICE_URL = "brokerServiceUrl";
	public static final String CONF_ZK_SERVER_URL = "zkServerUrl";
	public static final String CONF_REPLICATION_TOPIC_NAME = "replTopicName";
	public static final String CONF_REPLICATOR_TYPE = "replType";
	public static final String CONF_REPLICATOR_JAR_NAME = "replJar";
	public static final String CONF_REPLICATOR_MANAGER_CLASS_NAME = "replManagerClassName";
	public final static Map<String, ReplicatorManager> replicatorMap = Maps.newConcurrentMap();

	private String topicName;
	protected static final AtomicReferenceFieldUpdater<ReplicatorFunction, State> STATE_UPDATER = AtomicReferenceFieldUpdater
			.newUpdater(ReplicatorFunction.class, State.class, "state");
	private volatile State state = State.Stopped;

	public enum Action {
		Start, Stop, Restart;
	}

	private enum State {
		Stopped, Starting, Started;
	}

	@Override
	public Boolean process(String action, Context context) throws Exception {
		final String replProcessId = replicatorProcessId(context);
		this.topicName = context.getUserConfigValue(CONF_REPLICATION_TOPIC_NAME);
		log.info("[{}] Received replicator action {} for {}", replProcessId, action, this.topicName);
		ReplicatorManager replicatorManager = replicatorMap.get(replProcessId);

		Action processAction = Action.valueOf(action);

		switch (processAction) {
		case Stop:
			if (state == State.Started) {
				log.info("[{}] stopping replicator manager {}", replProcessId, this.topicName);
				close(replicatorManager);
			}
			break;
		case Restart:
			if (state == State.Stopped) {
				close(replicatorManager);
			}
			replicatorManager = null;
		case Start:
			if (state == State.Stopped) {
				startReplicator(context);
			}
			break;
		default:
			return false;
		}
		return true;
	}

	@Override
	public void init(Context context) throws Exception {
		startReplicator(context);
	}

	/**
	 * Based on Replicator type it initialize the replicator-manager which
	 * internally manages replicator-producer.
	 * 
	 * @param context
	 * @throws Exception
	 */
	public void startReplicator(Context context) throws Exception {

		final String replProcessId = replicatorProcessId(context);
		this.topicName = context.getUserConfigValue(CONF_REPLICATION_TOPIC_NAME);

		if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
			log.info("[{}] Replicator on topic {} is already in ", replProcessId, topicName, state);
			return;
		}

		String brokerServiceUrl = context.getUserConfigValue(CONF_BROKER_SERVICE_URL);
		String zkServerUrl = context.getUserConfigValue(CONF_ZK_SERVER_URL);
		String jarPath = context.getUserConfigValue(CONF_REPLICATOR_JAR_NAME);
		String replManagerClassName = context.getUserConfigValue(CONF_REPLICATOR_MANAGER_CLASS_NAME);

		log.info("[{}] starting replicator {}", replProcessId, topicName);

		ClassLoader classLoader = this.getClass().getClassLoader();
		try {
			if (StringUtils.isNotBlank(jarPath)) {
				URL[] urls = { new File(jarPath).toURL() };
				classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
			}
			ReplicatorManager replicatorManager = (ReplicatorManager) Class
					.forName(replManagerClassName, true, classLoader).getConstructor().newInstance();
			log.info("[{}] Successfully started replicator manager {}", replProcessId, topicName);
			ReplicatorConfig replConfig = new ReplicatorConfig();
			replConfig.setTopicName(topicName);
			replConfig.setBrokerServiceUrl(brokerServiceUrl);
			replConfig.setZkServiceUrl(zkServerUrl);
			replicatorManager.start(replConfig);
			replicatorMap.put(replProcessId, replicatorManager);
			STATE_UPDATER.set(this, State.Started);
		} catch (Exception e) {
			log.error("[{}} Failed to start replicator manager for {}-{}", replProcessId, topicName);
			throw e;
		} finally {
			// TODO: check if classLoader requires to close
			if (classLoader instanceof URLClassLoader) {
				((URLClassLoader) classLoader).close();
			}
		}
	}

	private void close(ReplicatorManager replicatorManager) {
		if (STATE_UPDATER.get(this) != State.Started) {
			log.info("Replicator is not started {}", this.topicName);
			return;
		}
		try {
			replicatorManager.stop();
			STATE_UPDATER.set(this, State.Stopped);
		} catch (Exception e) {
			log.warn("Failed to close replicator for {}", this.topicName);
		}
	}

	private String replicatorProcessId(Context context) {
		return context != null ? String.format("%s-%s", context.getInstanceId(), context.getFunctionId()) : null;
	}

	private static final Logger log = LoggerFactory.getLogger(ReplicatorFunction.class);
}
