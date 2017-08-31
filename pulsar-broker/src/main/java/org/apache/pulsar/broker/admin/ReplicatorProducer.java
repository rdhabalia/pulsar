package org.apache.pulsar.broker.admin;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.SendCallback;

public interface ReplicatorProducer {
    
    void send(Message message, SendCallback callback);

}
