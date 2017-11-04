/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.DefaultThreadFactory;

public class RecyclerTest  {
    private static final Logger log = LoggerFactory.getLogger(RecyclerTest.class);

   
    /**
     * 
     * jmap -histo <PID> | grep AddCompletion
     * 
     * OP:
     * io.netty.recycler.maxCapacity.default  : total AddCompletion-objects
     * 1. 1000:                                 5984
     * 2. 2000:                                 11968
     * 3. 3000:                                 17952
     * @throws Exception
     */
    
    @Test
    public void testRecycler() throws Exception {
        System.setProperty("io.netty.recycler.maxCapacity.default", "3000");

        ScheduledExecutorService executor1 = Executors.newScheduledThreadPool(4, new DefaultThreadFactory("thread-A"));
        int totalObjects = 50_00_000;
        CountDownLatch addLatch = new CountDownLatch(totalObjects);
        CountDownLatch getLatch = new CountDownLatch(totalObjects);
        BlockingQueue<AddCompletion> queue = new LinkedBlockingQueue<>(totalObjects);
        for (int i = 0; i < totalObjects; i++) {
            final int field = i;
            executor1.submit(() -> {
                AddCompletion add = AddCompletion.get(field);
                queue.add(add);
                addLatch.countDown();
            });
        }
        addLatch.await();
        for (int i = 0; i < totalObjects; i++) {
            executor1.submit(() -> {
                queue.poll().recycle();
                ;
                getLatch.countDown();
            });
        }
        getLatch.await();
        System.out.println("Finish with recylcing " + totalObjects);
        long start = System.currentTimeMillis();
        System.out.println("Starting gc....");
        System.gc();
        long end = System.currentTimeMillis();
        System.out.println("Finish GC.." + (end - start));
        CountDownLatch latch2 = new CountDownLatch(1);
        latch2.await();

    }
 
    
    static class AddCompletion  {
        private long field1;
        public long getField1() {
            return field1;
        }


        public void setField1(long field1) {
            this.field1 = field1;
        }

        private final Handle recyclerHandle;
        private static final Recycler<AddCompletion> RECYCLER = new Recycler() {
            protected AddCompletion newObject(Handle handle) {
                return new AddCompletion(handle);
            }
        };

        public static AddCompletion get(long field1) {
            AddCompletion addCompletion = (AddCompletion) RECYCLER.get();
            addCompletion.field1 = field1;
            return addCompletion;
        }


        private AddCompletion(Handle handle) {
            this.recyclerHandle = handle;
        }

        public void recycle() {
            this.field1 = 0L;
            RECYCLER.recycle(this, this.recyclerHandle);
        }
    }
    
}