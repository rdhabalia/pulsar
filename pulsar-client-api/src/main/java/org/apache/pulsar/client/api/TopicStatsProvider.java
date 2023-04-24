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
package org.apache.pulsar.client.api;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicStats;

/**
 * Stats provider provides API to fetch topic's stats and internal-stats.
 */
@InterfaceAudience.Public
public interface TopicStatsProvider {

    /**
     * @return the topic stats
     */
    CompletableFuture<TopicStats>  getStats();
    
    CompletableFuture<PersistentTopicInternalStats>  getInternalStats();
    
    CompletableFuture<PartitionedTopicStats>  getPartitionTopicStats();
    
    CompletableFuture<PartitionedTopicInternalStats>  getInternalPartitionTopicStats();

    
}
