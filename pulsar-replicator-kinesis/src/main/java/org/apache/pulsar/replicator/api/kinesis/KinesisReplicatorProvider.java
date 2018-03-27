package org.apache.pulsar.replicator.api.kinesis;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.apache.pulsar.replicator.api.ReplicatorProvider;
import org.apache.pulsar.replicator.auth.AuthorizationKeyStore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class KinesisReplicatorProvider implements ReplicatorProvider {

	public KinesisReplicatorProvider() {

	}

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
	public CompletableFuture<ReplicatorProducer> createProducerAsync(String topicName,
			ReplicatorPolicies replicatorPolicies) {

		if (replicatorPolicies == null || replicatorPolicies.topicNameMapping == null) {
			throw new IllegalStateException("Topic-mappong of ReplicatorPolicies can't be empty");
		}
		String streamParam = replicatorPolicies.topicNameMapping.get(TopicName.get(topicName).getLocalName());
		if (StringUtils.isBlank(streamParam) || !streamParam.contains(":")) {
			throw new IllegalStateException("invalid stream param [streamName:regionName] " + streamParam);
		}
		int splitIndex = streamParam.lastIndexOf(":");
		String streamName = streamParam.substring(0, splitIndex);
		String regionName = streamParam.substring(splitIndex + 1);
		Region region = Region.getRegion(Regions.fromName(regionName));
		AWSCredentials credentials = fetchCredential(topicName, replicatorPolicies);
		return CompletableFuture.completedFuture(new KinesisReplicatorProducer(streamName, region, credentials));
	}

	private AWSCredentials fetchCredential(String topicName, ReplicatorPolicies replicatorPolicies) {
		String pluginName = replicatorPolicies.authPluginName;
		String paramName = replicatorPolicies.authParamName;
		try {
			Class<?> clazz = Class.forName(pluginName);
			Constructor<?> ctor = clazz.getConstructor();
			AuthorizationKeyStore authKeyStore = (AuthorizationKeyStore) ctor.newInstance(new Object[] {});
			Map<String, String> crendentialMap = ObjectMapperFactory.getThreadLocal().readValue(authKeyStore.getAuthData(topicName, replicatorPolicies), Map.class);
			return new AWSCredentials() {
				@Override
				public String getAWSAccessKeyId() {
					return crendentialMap.get("accessKey");
				}
				@Override
				public String getAWSSecretKey() {
					return crendentialMap.get("secretKey");
				}
				
			};
		} catch (Exception e) {
			// TODO:
			e.printStackTrace();
		}
		
		return null;
	}

}
