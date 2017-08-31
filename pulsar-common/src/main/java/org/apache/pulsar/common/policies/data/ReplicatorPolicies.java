package org.apache.pulsar.common.policies.data;

import java.util.Map;

public class ReplicatorPolicies {

    public ReplicatorType type;
    public String name;
    public Map<String,String> topicProperties; // eg: regionName=us-east-2
    public Map<String,String> credentialProperties; //eg: aws_access_key_id=id1, aws_secret_access_key=key1 (broker will encrypt it before storing into zk)
    
    
}
