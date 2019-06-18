package org.apache.kafka.clients.consumer;

import java.util.Properties;

public class PulsarConsumerConfig extends kafka.consumer.ConsumerConfig{

    public PulsarConsumerConfig(Properties originalProps) {
        super(originalProps);
    }

}
