/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperBkClientFactoryImpl implements ZooKeeperClientFactory {
    public static final Charset ENCODING_SCHEME = Charset.forName("UTF-8");

    @Override
    public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType, int zkSessionTimeoutMillis) {
        // Create a normal ZK client

        boolean canBeReadOnly = sessionType == SessionType.AllowReadOnly;

        CompletableFuture<ZooKeeper> future = new CompletableFuture<>();
        try {
            CompletableFuture<Void> internalFuture = new CompletableFuture<>();

            try {
                Constructor<ZooKeeperClient> cn = ZooKeeperClient.class.getDeclaredConstructor(String.class,
                        Integer.class, ZooKeeperWatcherBase.class, RetryPolicy.class);
                cn.setAccessible(true);
                ZooKeeperWatcher watcherManager = new ZooKeeperWatcher(zkSessionTimeoutMillis, canBeReadOnly,
                        internalFuture);
                final ZooKeeperClient zk = cn.newInstance(serverList, zkSessionTimeoutMillis, watcherManager,
                        new BoundExponentialBackoffRetryPolicy(zkSessionTimeoutMillis, zkSessionTimeoutMillis, 0));

                internalFuture.thenRun(() -> {
                    log.info("ZooKeeper session established: {}", zk);
                    future.complete(zk);
                }).exceptionally((exception) -> {
                    log.error("Failed to establish ZooKeeper session: {}", exception.getMessage());
                    future.completeExceptionally(exception);
                    return null;
                });

                internalFuture.complete(null);
            } catch (Exception ex) {
                internalFuture.completeExceptionally(ex);
            }

        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    static class ZooKeeperWatcher extends ZooKeeperWatcherBase {

        private boolean canBeReadOnly;
        private CompletableFuture<Void> internalFuture;

        public ZooKeeperWatcher(int zkSessionTimeOut, boolean canBeReadOnly, CompletableFuture<Void> internalFuture) {
            super(zkSessionTimeOut);
            this.canBeReadOnly = canBeReadOnly;
            this.internalFuture = internalFuture;
        }

        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.None) {
                switch (event.getState()) {

                case ConnectedReadOnly:
                    checkArgument(canBeReadOnly);
                    // Fall through
                case SyncConnected:
                    // ZK session is ready to use
                    internalFuture.complete(null);
                    break;

                case Expired:
                    internalFuture.completeExceptionally(KeeperException.create(KeeperException.Code.SESSIONEXPIRED));
                    break;

                default:
                    log.warn("Unexpected ZK event received: {}", event);
                    break;
                }
            }
        }

    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperBkClientFactoryImpl.class);
}
