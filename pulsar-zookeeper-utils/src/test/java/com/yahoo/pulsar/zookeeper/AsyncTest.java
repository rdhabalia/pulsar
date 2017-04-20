package com.yahoo.pulsar.zookeeper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.util.concurrent.DefaultThreadFactory;

public class AsyncTest {

    public static void main(String[] args) throws Exception{

        ExecutorService executor = Executors.newFixedThreadPool(5, new DefaultThreadFactory("myThread"));

        CompletableFuture<Integer> future1 = new CompletableFuture<>();
        CompletableFuture<Integer> future2 = new CompletableFuture<>();
        
        // Main thread acquires a lock
        System.out.println("lock :" + Thread.currentThread());
        
        executor.submit(() -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            future1.complete(1);
        });

        executor.submit(() -> {
            future2.complete(1);
        });

        // Both result have been processed by executor thread: 
        // but future1-result is processed by executor-thread and future2-result processed by main-thread 
        future1.thenAccept(res -> System.out.println("Future1 result-trying to lock " + Thread.currentThread()));
        // future2 result will be processed by [main] thread which tries to take lock before unlocking which creates a deadlock
        future2.thenAccept(res -> System.out.println("Future2 result-trying to lock " + Thread.currentThread()));
        
        System.out.println("Unlock :" + Thread.currentThread());
        
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
    /**
     * Output:
     * 
     * lock :Thread[main,5,main]
     * Future2 result-trying to lock Thread[main,5,main]
     * Unlock :Thread[main,5,main]
     * Future1 result-trying to lock Thread[myThread-1-1,5,main]
     * 
     */
}
