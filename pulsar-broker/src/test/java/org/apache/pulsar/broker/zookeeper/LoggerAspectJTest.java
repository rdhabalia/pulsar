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
package org.apache.pulsar.broker.zookeeper;

import org.apache.pulsar.broker.zookeeper.aspectj.LoggerAspect;
import org.aspectj.weaver.loadtime.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.ea.agentloader.AgentLoader;

public class LoggerAspectJTest {

    static {
        AgentLoader.loadAgentClass(Agent.class.getName(), null);
    }

    private static final Logger log = LoggerFactory.getLogger(LoggerAspectJTest.class);
    
    @Test
    public void testLoggerAspect() throws Exception {
        System.out.println(log.getClass().getName());
        final String topicName = "test";
        LoggerAspect.addDebugToken(topicName);
        log.debug("trying to test {}-{}",topicName,1);
        log.debug("trying to test {}-{}","this will not be logged",1);
    }
}
