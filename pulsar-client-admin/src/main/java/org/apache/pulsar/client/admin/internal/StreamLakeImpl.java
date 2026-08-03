/*
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
package org.apache.pulsar.client.admin.internal;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.StreamLake;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.StreamLakeQueryResult;

/**
 * Client-side implementation of the {@link StreamLake} admin API: posts the SQL string to the broker's
 * {@code POST /admin/v3/streamlake/{tenant}/{namespace}/query} endpoint and deserializes the
 * {@link StreamLakeQueryResult}.
 */
public class StreamLakeImpl extends BaseResource implements StreamLake {

    private final WebTarget adminV3StreamLake;

    public StreamLakeImpl(WebTarget web, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        this.adminV3StreamLake = web.path("admin/v3/streamlake");
    }

    @Override
    public StreamLakeQueryResult query(String tenant, String namespace, String sql)
            throws PulsarAdminException {
        return sync(() -> queryAsync(tenant, namespace, sql));
    }

    @Override
    public CompletableFuture<StreamLakeQueryResult> queryAsync(String tenant, String namespace, String sql) {
        WebTarget path = adminV3StreamLake.path(tenant).path(namespace).path("query");
        final CompletableFuture<StreamLakeQueryResult> future = new CompletableFuture<>();
        try {
            request(path).async().post(Entity.entity(sql, MediaType.TEXT_PLAIN),
                    new InvocationCallback<StreamLakeQueryResult>() {
                        @Override
                        public void completed(StreamLakeQueryResult result) {
                            future.complete(result);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
