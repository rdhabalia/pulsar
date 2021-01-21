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
package org.apache.pulsar.broker.admin.impl;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;


public class BaseResources<T> {

    @Getter
    private final MetadataStoreExtended store;
    @Getter
    private final MetadataCache<T> cache;

    public BaseResources(MetadataStoreExtended store, Class<T> clazz) {
        this.store = store;
        this.cache = store.getMetadataCache(clazz);
    }

    public CompletableFuture<List<String>> getChildren(String path) {
        return cache.getChildren(path);
    }

    public Optional<T> get(String path) throws PulsarServerException {
        try {
            return getAsync(path).get();
        } catch (InterruptedException e) {
            throw new PulsarServerException("Failed to get data from " + path, e);
        } catch (ExecutionException e) {
            throw new PulsarServerException("Failed to get data from " + path, e.getCause());
        }
    }

    public CompletableFuture<Optional<T>> getAsync(String path) {
        return cache.get(path);
    }

    public CompletableFuture<Void> set(String path, Function<T, T> modifyFunction) {
        return cache.readModifyUpdate(path, modifyFunction);
    }

    public CompletableFuture<Void> create(String path, T data) {
        return cache.readModifyUpdateOrCreate(path, t -> data);
    }

    public CompletableFuture<Void> deleteAsync(String path) {
        return cache.delete(path);
    }

    public CompletableFuture<Boolean> exists(String path) {
        return cache.exists(path);
    }
}
