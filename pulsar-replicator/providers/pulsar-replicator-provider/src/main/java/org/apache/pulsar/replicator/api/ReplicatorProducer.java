package org.apache.pulsar.replicator.api;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Message;

public interface ReplicatorProducer {
	CompletableFuture<Void> send(Message message);

	void close();

}
