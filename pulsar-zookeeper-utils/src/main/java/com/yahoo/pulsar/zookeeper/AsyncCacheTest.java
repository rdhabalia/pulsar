package com.yahoo.pulsar.zookeeper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class AsyncCacheTest {

    protected final AsyncLoadingCache<String, Integer> dataCache;

    public AsyncCacheTest() {
        dataCache = Caffeine.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).buildAsync((key, executor1) -> null);
    }

    public static void main(String[] args) throws Exception {
        AsyncCacheTest test = new AsyncCacheTest();
        final int totalThreads = Runtime.getRuntime().availableProcessors();
        final ScheduledExecutorService schedueler = Executors.newScheduledThreadPool(totalThreads);

        System.out.println("Total threads = " + totalThreads);
        final int totalKeys = 5;// 20_000;// 1;
        CountDownLatch latch = new CountDownLatch(totalKeys * 2);

        for (int i = 0; i < totalKeys; i++) {
            final String key = Integer.toString(i);
            CompletableFuture<Integer> result1 = test.get(schedueler, key);
            CompletableFuture<Integer> result2 = test.get(schedueler, key);
            schedueler.submit(() -> {
                try {
                    System.out.println("waiting for res1 = "+key);
                    System.out.println("res1= " + result1.get());
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            schedueler.submit(() -> {
                try {
                    System.out.println("waiting for res2 = "+key);
                    System.out.println("res2= " + result2.get());
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        latch.await();
        System.out.println("finish");
    }

    private CompletableFuture<Integer> get(ScheduledExecutorService schedueler, String key) {
        return this.dataCache.get(key, (p, executor) -> {
            final CompletableFuture<Integer> future = new CompletableFuture<>();
            schedueler.schedule(() -> {
                System.out.println("Future completed =" + key);
                future.complete(Integer.parseInt(key));
            }, 5, TimeUnit.SECONDS);
            return future;
        });
    }
}
