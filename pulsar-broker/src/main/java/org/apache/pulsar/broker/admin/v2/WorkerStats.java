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
package org.apache.pulsar.broker.admin.v2;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.functions.proto.InstanceCommunication.Metrics;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.WorkerImpl;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

@Slf4j
@Path("/worker-stats")
public class WorkerStats extends AdminResource implements Supplier<WorkerService> {

    private final WorkerImpl worker;

    public WorkerStats() {
        this.worker = new WorkerImpl(this);
    }

    @Override
    public WorkerService get() {
        return pulsar().getWorkerService();
    }

    @GET
    @Path("/metrics")
    @ApiOperation(value = "Gets the metrics for Monitoring", notes = "Request should be executed by Monitoring agent on each worker to fetch the worker-metrics", response = org.apache.pulsar.common.stats.Metrics.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 401, message = "Don't have admin permission") })
    public Collection<org.apache.pulsar.common.stats.Metrics> getMetrics() throws Exception {
        return worker.getWorkerMetrics(clientAppId());
    }

    @GET
    @Path("/functionsmetrics")
    @ApiOperation(value = "Get metrics for all functions owned by worker", notes = "Requested should be executed by Monitoring agent on each worker to fetch the metrics", response = Metrics.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "Worker service is not running") })
    public Response getStats() throws IOException {
        return worker.getFunctionsMetrics(clientAppId());
    }
}
