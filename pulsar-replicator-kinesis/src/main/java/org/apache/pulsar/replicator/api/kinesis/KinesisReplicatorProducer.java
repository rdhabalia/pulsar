package org.apache.pulsar.replicator.api.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.replicator.api.ReplicatorProducer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class KinesisReplicatorProducer implements ReplicatorProducer {

	private String streamName;
	private KinesisProducer kinesisProducer;

	public KinesisReplicatorProducer(String streamName, Region region, AWSCredentials credentials) {
		this.streamName = streamName;

		AmazonKinesisClient kinesis = new AmazonKinesisClient(credentials);
		kinesis.setRegion(region);

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		/*DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

		String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
		if (!"ACTIVE".equals(streamStatus)) {
			// TODO: throw exception
			throw new IllegalStateException(streamName + " is not activated");
		}*/

		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(region.getName());
		AWSCredentialsProvider credentialProvider = new AWSCredentialsProvider() {
			@Override
			public AWSCredentials getCredentials() {
				return credentials;
			}

			@Override
			public void refresh() {
				// TODO : no-op
			}
		};
		config.setCredentialsProvider(credentialProvider);
		this.kinesisProducer = new KinesisProducer(config);

	}

	@Override
	public CompletableFuture<Void> send(Message message) {
		System.out.println("kinesis producer: " + new String(message.getData()));
		CompletableFuture<Void> future = new CompletableFuture<>();
		ListenableFuture<UserRecordResult> addRecordResult = kinesisProducer.addUserRecord(this.streamName,
				"partitioned-key", ByteBuffer.wrap(message.getData()));
		Futures.addCallback(addRecordResult, new FutureCallback<UserRecordResult>() {
			@Override
			public void onSuccess(UserRecordResult result) {
				future.complete(null);
			}
			@Override
			public void onFailure(Throwable ex) {
				future.completeExceptionally(ex);
			}
		}, MoreExecutors.sameThreadExecutor());
		return future;
	}

	@Override
	public void close() {
		if (kinesisProducer != null) {
			kinesisProducer.flush();
			kinesisProducer.destroy();
		}
	}
	
}
