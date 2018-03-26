package org.apache.pulsar.replicator.api.kinesis;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.SendCallback;
import org.apache.pulsar.replicator.api.ReplicatorProducer;

public class KinesisReplicatorProducer implements ReplicatorProducer {

	@Override
	public CompletableFuture<Void> send(Message message) {
		System.out.println("kinesis producer: " + new String(message.getData()));
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
