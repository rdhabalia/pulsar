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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.StreamLake;
import org.apache.pulsar.client.admin.StreamLakeQueryResultHandler;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.StreamLakeQueryResult;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Client-side implementation of the {@link StreamLake} admin API: posts the SQL string to the broker's
 * {@code POST /admin/v3/streamlake/{tenant}/{namespace}/query} endpoint and reads the <b>NDJSON</b>
 * response stream (line 1 = column names, each following line = one row) incrementally, so a large
 * result never has to be buffered whole on the client.
 */
public class StreamLakeImpl extends BaseResource implements StreamLake {

    private static final ObjectMapper MAPPER = ObjectMapperFactory.getMapper().getObjectMapper();
    private static final TypeReference<List<String>> COLUMNS = new TypeReference<List<String>>() { };
    private static final TypeReference<List<Object>> ROW = new TypeReference<List<Object>>() { };

    private final WebTarget adminV3StreamLake;

    public StreamLakeImpl(WebTarget web, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        this.adminV3StreamLake = web.path("admin/v3/streamlake");
    }

    @Override
    public void query(String tenant, String namespace, String sql, StreamLakeQueryResultHandler handler)
            throws PulsarAdminException {
        WebTarget path = adminV3StreamLake.path(tenant).path(namespace).path("query");
        Response response = null;
        try {
            response = request(path).accept("application/x-ndjson", MediaType.APPLICATION_JSON)
                    .post(Entity.entity(sql, MediaType.TEXT_PLAIN));
            if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
                throw getApiException(response);
            }
            try (InputStream is = response.readEntity(InputStream.class);
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String header = reader.readLine();
                handler.columns(header == null || header.isEmpty()
                        ? Collections.emptyList() : MAPPER.readValue(header, COLUMNS));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        handler.row(MAPPER.readValue(line, ROW));
                    }
                }
            }
        } catch (PulsarAdminException e) {
            throw e;
        } catch (Exception e) {
            throw new PulsarAdminException(e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public StreamLakeQueryResult query(String tenant, String namespace, String sql)
            throws PulsarAdminException {
        long t0 = System.nanoTime();
        List<List<Object>> rows = new ArrayList<>();
        List<List<String>> columnsHolder = new ArrayList<>(1);
        query(tenant, namespace, sql, new StreamLakeQueryResultHandler() {
            @Override
            public void columns(List<String> columns) {
                columnsHolder.add(columns);
            }

            @Override
            public void row(List<Object> row) {
                rows.add(row);
            }
        });
        List<String> columns = columnsHolder.isEmpty() ? Collections.emptyList() : columnsHolder.get(0);
        return new StreamLakeQueryResult(columns, rows, (System.nanoTime() - t0) / 1_000_000);
    }

    @Override
    public CompletableFuture<StreamLakeQueryResult> queryAsync(String tenant, String namespace, String sql) {
        CompletableFuture<StreamLakeQueryResult> future = new CompletableFuture<>();
        try {
            future.complete(query(tenant, namespace, sql));
        } catch (PulsarAdminException e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
