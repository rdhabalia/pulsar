package org.apache.pulsar.replicator.api.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class KinesisTest {

	public static void main(String[] args) {
		
		try {
			String topicName = "persistent://cms-java-systest/global/replicator/replTopic1";
			Region region = Region.getRegion(Regions.fromName("us-west-2"));
			AWSCredentials credentials = new AWSCredentials() {
				@Override
				public String getAWSAccessKeyId() {
					return "key";
				}
				@Override
				public String getAWSSecretKey() {
					return "secret";
				}
				
			};
			String streamName = "function-perf";
			
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
			KinesisProducer kinesisProducer = new KinesisProducer(config);
			CompletableFuture<Void> future = new CompletableFuture<>();
			byte[] message = "1".getBytes();
			ListenableFuture<UserRecordResult> addRecordResult = kinesisProducer.addUserRecord(streamName,
					"partitioned-key", ByteBuffer.wrap(message));
			System.out.println("sent a kinesis message" + addRecordResult.get());
			Futures.addCallback(addRecordResult, new FutureCallback<UserRecordResult>() {
				@Override
				public void onSuccess(UserRecordResult result) {
					System.out.println("successfully sent ="+new String(message));
					future.complete(null);
				}
				@Override
				public void onFailure(Throwable ex) {
					System.out.println("failed sent ="+new String(message));
					System.out.println("Failed to sent ="+new String(message));
					future.completeExceptionally(ex);
				}
			}, MoreExecutors.sameThreadExecutor());
		
			System.out.println("completed ="+future.get());
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
