package org.apache.pulsar.replicator.auth;

import org.apache.pulsar.common.policies.data.ReplicatorPolicies;

public interface AuthorizationKeyStore {

	/**
	 * json param
	 * 
	 * @param authData
	 */
	void setAuthData(String authData);
	
	/**
	 * 
	 * @param topicName
	 * @param replicatorPolicies
	 * @return json param
	 */
	String getAuthData(String topicName, ReplicatorPolicies replicatorPolicies);
	
}
