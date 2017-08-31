package org.apache.pulsar.broker.admin;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ReplicatorProvider {
    
    /**
     * Validates topic-properties and credential-properties which must be compliance with 
     * eg: for kinessis
     * 1. topicProperties must contain: key=> eg: regionName = us-east-2
     * 2. credentialProperties must contain key=> eg: awsAccessKeyId=123, awsSecretAccessKey=123
     * 
     * @param topicProperties
     * @param credentialProperties
     * @throws IllegalArgumentException
     */
    public void validateProperties(Map<String,String> topicProperties, Map<String,String> credentialProperties) throws IllegalArgumentException;
    
    /**
     * Create Replicator-producer async which will publish messages to targeted replication-system such as 
     *  eg: pulsar-repl-cluster, kinessis-server, kafka-server
     * 
     * @param topic
     * @param topicProperties
     * @param credentialProperties
     * @return 
     */
    public CompletableFuture<ReplicatorProducer> createProducerAsync(final String topic, Map<String,String> topicProperties, Map<String,String> credentialProperties);

}
