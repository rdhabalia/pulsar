package org.apache.pulsar.replicator.api.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
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
	private volatile long partitionKeySequence = 0;

	public KinesisReplicatorProducer(String streamName, Region region, AWSCredentials credentials) {
		this.streamName = streamName;

		AmazonKinesisClient kinesis = new AmazonKinesisClient(credentials);
		kinesis.setRegion(region);

		/*
		 * DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		 * describeStreamRequest.setStreamName(streamName); DescribeStreamResult
		 * describeStreamResponse = kinesis.describeStream(describeStreamRequest);
		 * 
		 * String streamStatus =
		 * describeStreamResponse.getStreamDescription().getStreamStatus(); if
		 * (!"ACTIVE".equals(streamStatus)) { // TODO: throw exception throw new
		 * IllegalStateException(streamName + " is not activated"); }
		 */

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
		config.setAggregationEnabled(true);
		config.setMetricsLevel("none");
		config.setLogLevel("info");
		config.setFailIfThrottled(true);
		this.kinesisProducer = new KinesisProducer(config);
		log.info("Kinesis producer created on {}", streamName);

	}

	@Override
	public CompletableFuture<Void> send(Message message) {
		log.info("sending message replicator {}-{}", this.streamName, message.getData().length);
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			ListenableFuture<UserRecordResult> addRecordResult = kinesisProducer.addUserRecord(this.streamName,
					Long.toString(partitionKeySequence++), ByteBuffer.wrap(message.getData()));
			
			if(partitionKeySequence<-1) {
				log.info("sent a kinesis message part-key{} - {}", partitionKeySequence, addRecordResult.get());	
			} else {
				log.info("sent a kinesis message part-key{} - {}", partitionKeySequence);//, addRecordResult.get());	
			}
			Futures.addCallback(addRecordResult, new FutureCallback<UserRecordResult>() {
				@Override
				public void onSuccess(UserRecordResult result) {
					log.info("successfully sent = {} size", message.getData().length);
					future.complete(null);
				}

				@Override
				public void onFailure(Throwable ex) {
					log.info("Failed to sent = {} size", message.getData().length);
					future.completeExceptionally(ex);
				}
			}, MoreExecutors.sameThreadExecutor());
		} catch (Exception e) {
			e.printStackTrace();
			log.info("Failed to sent = {} size", message.getData().length, e);
			future.completeExceptionally(e);
		}

		return future;
	}

	@Override
	public void close() {
		System.out.println("****** closing replicator producer for *****" + streamName);
		log.info("closing replicator {}", streamName);
		if (kinesisProducer != null) {
			kinesisProducer.flush();
			kinesisProducer.destroy();
		}
	}

	private static final Logger log = LoggerFactory.getLogger(KinesisReplicatorProducer.class);
}
