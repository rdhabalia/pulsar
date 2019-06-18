package org.apache.kafka.clients.consumer;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

public class PulsarMessageAndMetadata<K, V> extends MessageAndMetadata<K, V> {

    private static final long serialVersionUID = 1L;
    private final String topic;
    private final int partition;
    private final Message rawMessage;
    private final long offset;
    private final V value;
    private final Decoder<K> keyDecoder;
    private final Decoder<V> valueDecoder;
    
    public PulsarMessageAndMetadata(String topic, int partition, Message rawMessage, long offset, Decoder<K> keyDecoder,
            Decoder<V> valueDecoder, V value) {
        super(topic, partition, rawMessage, offset, keyDecoder, valueDecoder);
        this.topic = topic;
        this.partition = partition;
        this.rawMessage = rawMessage;
        this.offset = offset;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.value = value;
    }

    @Override
    public  String topic() {
        return topic;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public V message() {
        return this.value;
    }

    @Override
    public Decoder<V> valueDecoder() {
        return this.valueDecoder;
    }

    @Override
    public Decoder<K> keyDecoder() {
        return this.keyDecoder;
    }

}
