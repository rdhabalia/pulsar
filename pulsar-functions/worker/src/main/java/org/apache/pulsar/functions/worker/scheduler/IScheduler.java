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
package org.apache.pulsar.functions.worker.scheduler;

import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.Instance;

import java.util.List;
import java.util.Set;

public interface IScheduler {

    /**
     * Scheduler schedules assignments to appropriate workers and adds into #resultAssignments
     * 
     * @param resultAssignments
     *            scheduler adds new assignents into the list
     * @param unassignedFunctionInstances
     *            all unassigned instances
     * @param currentAssignments
     *            current assignments
     * @param workers
     * @return
     */
    List<Assignment> schedule(List<Assignment> resultAssignments, List<Instance> unassignedFunctionInstances,
            List<Assignment> currentAssignments, Set<String> workers);
}
