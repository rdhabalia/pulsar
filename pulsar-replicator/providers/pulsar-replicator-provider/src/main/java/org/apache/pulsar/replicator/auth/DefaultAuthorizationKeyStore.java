package org.apache.pulsar.replicator.auth;

import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class DefaultAuthorizationKeyStore implements AuthorizationKeyStore{

	@Override
	public void setAuthData(String authData) {
		// TODO No-op
	}

	@Override
	public String getAuthData(String topicName, ReplicatorPolicies replicatorPolicies) {
		if(replicatorPolicies!=null && replicatorPolicies.replicationProperties!=null) {
			try {
				return ObjectMapperFactory.getThreadLocal().writeValueAsString(replicatorPolicies.replicationProperties);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	
}
