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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeQueryCoordinator;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeQueryService;
import org.apache.pulsar.broker.web.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin REST endpoint for submitting a StreamLake SQL query and receiving result rows. The query is
 * planned + executed by the broker-side {@link StreamLakeQueryCoordinator}: table names in the SQL are
 * resolved to their topics <b>in the request's namespace</b>, and single-table scans and two-table
 * inner joins are supported. This is the transport behind {@code pulsar-admin streamlake query}.
 *
 * <p>The heavy scan/join runs on the broker executor (off the Jersey IO thread) and the result is
 * resumed asynchronously. Tables must be StreamLake topics owned by this broker (the demo/standalone
 * case); a table that is not found or not a StreamLake topic yields a 400.
 */
@Path("/streamlake")
@Tag(name = "streamlake")
@Produces(MediaType.APPLICATION_JSON)
public class StreamLakeQuery extends AdminResource {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeQuery.class);

    @POST
    @Path("/{tenant}/{namespace}/query")
    @Consumes(MediaType.TEXT_PLAIN)
    @Operation(summary = "Run a StreamLake SQL query (single-table scan or inner join) over the "
            + "namespace's tables and return the result rows.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Query result (columns + rows)."),
            @ApiResponse(responseCode = "400", description = "Malformed query or unknown/!StreamLake table.")
    })
    public void query(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            String sql) {
        validateNamespaceName(tenant, namespace);
        if (sql == null || sql.trim().isEmpty()) {
            asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, "Empty query"));
            return;
        }
        // Run the (blocking) scan/join off the request thread.
        pulsar().getExecutor().execute(() -> {
            try {
                StreamLakeQueryCoordinator coordinator = new StreamLakeQueryCoordinator(
                        table -> resolveService(tenant, namespace, table));
                asyncResponse.resume(coordinator.executeSql(sql));
            } catch (IllegalArgumentException e) {
                asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, e.getMessage()));
            } catch (Exception e) {
                log.warn("StreamLake query failed for {}/{}: {}", tenant, namespace, e.toString());
                asyncResponse.resume(new RestException(e));
            }
        });
    }

    // Resolve a table name to its per-topic query service (loading the topic on this broker if needed).
    private StreamLakeQueryService resolveService(String tenant, String namespace, String table) {
        String topicName = "persistent://" + tenant + "/" + namespace + "/" + table;
        try {
            Optional<Topic> ref = pulsar().getBrokerService().getTopicReference(topicName);
            if (ref.isEmpty()) {
                ref = pulsar().getBrokerService().getTopic(topicName, false).get();
            }
            if (ref.isEmpty() || !(ref.get() instanceof PersistentTopic)) {
                return null;
            }
            return ((PersistentTopic) ref.get()).getStreamLakeQueryService();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to resolve table " + table + ": " + e.getMessage(), e);
        }
    }
}
