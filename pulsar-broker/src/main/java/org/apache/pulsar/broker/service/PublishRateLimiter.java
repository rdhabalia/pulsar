package org.apache.pulsar.broker.service;

import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PublishRateLimiter {

    static PublishRateLimiter DISABLED_RATE_LIMITER = PublishRateLimiterDisable.DISABLED_RATE_LIMITER;

    void checkPublishRate();

    void incrementPublishCount(long msgSizeInBytes, int numOfMessages);

    boolean resetPublishCount();

    boolean isPublishRateExceeded();

    void update(Policies policies, String clusterName);

}

class PublishRateLimiterImpl implements PublishRateLimiter {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    protected volatile boolean publishRateExceeded = false;
    protected volatile LongAdder currentPublishMsgCount = null;
    protected volatile LongAdder currentPublishByteCount = null;
    private static final Logger log = LoggerFactory.getLogger(PublishRateLimiterImpl.class);

    public PublishRateLimiterImpl(Policies policies, String clusterName) {
        update(policies, clusterName);
        currentPublishMsgCount = new LongAdder();
        currentPublishByteCount = new LongAdder();
    }

    @Override
    public void checkPublishRate() {
        if (this.publishThrottlingEnabled && !publishRateExceeded) {
            long currentPublishMsgRate = this.currentPublishMsgCount.sum();
            long currentPublishByteRate = this.currentPublishByteCount.sum();
            if ((this.publishMaxMessageRate > 0 && currentPublishMsgRate > this.publishMaxMessageRate)
                    || (this.publishMaxByteRate > 0 && currentPublishByteRate > this.publishMaxByteRate)) {
                publishRateExceeded = true;
            }
        }
    }

    @Override
    public void incrementPublishCount(long msgSizeInBytes, int numOfMessages) {
        if (this.publishThrottlingEnabled) {
            System.out.println("incrementing "+msgSizeInBytes);
            this.currentPublishMsgCount.add(numOfMessages);
            this.currentPublishByteCount.add(msgSizeInBytes);
        }
    }

    @Override
    public boolean resetPublishCount() {
        if (this.publishThrottlingEnabled || this.publishRateExceeded) {
            this.currentPublishMsgCount.reset();
            this.currentPublishByteCount.reset();
            this.publishRateExceeded = false;
            return true;
        }
        return false;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return publishRateExceeded;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        final PublishRate maxPublishRate = policies.publish_max_message_rate != null
                ? policies.publish_max_message_rate.get(clusterName)
                : null;
        if (maxPublishRate != null
                && (maxPublishRate.publishThrottlingRateInMsg > 0 || maxPublishRate.publishThrottlingRateInByte > 0)) {
            this.publishThrottlingEnabled = true;
            this.publishMaxMessageRate = Math.max(maxPublishRate.publishThrottlingRateInMsg, 0);
            this.publishMaxByteRate = Math.max(maxPublishRate.publishThrottlingRateInByte, 0);
        } else {
            resetPublishCount();
            this.publishMaxMessageRate = 0;
            this.publishMaxByteRate = 0;
            this.publishThrottlingEnabled = false;
        }
    }
}

class PublishRateLimiterDisable implements PublishRateLimiter {

    public static final PublishRateLimiterDisable DISABLED_RATE_LIMITER = new PublishRateLimiterDisable();

    @Override
    public void checkPublishRate() {
        // No-op
    }

    @Override
    public void incrementPublishCount(long msgSizeInBytes, int numOfMessages) {
        // No-op
    }

    @Override
    public boolean resetPublishCount() {
        // No-op
        return false;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        // No-op
    }

}
