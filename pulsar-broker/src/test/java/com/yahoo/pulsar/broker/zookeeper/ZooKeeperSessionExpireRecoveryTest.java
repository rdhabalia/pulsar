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
package com.yahoo.pulsar.broker.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.zookeeper.KeeperException.Code;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.yahoo.pulsar.client.admin.PulsarAdminException;

public class ZooKeeperSessionExpireRecoveryTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Verify we are able to recover when receiving a SessionExpired event on global ZK session
     */
    @Test
    public void testSessionExpired() throws Exception {
        admin.clusters().createCluster("my-cluster", new ClusterData("test-url"));

        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("my-cluster"));

        dataMockZookKeeper.failNow(Code.SESSIONEXPIRED);

        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("my-cluster"));

        try {
            admin.clusters().createCluster("my-cluster-2", new ClusterData("test-url"));
            fail("Should have failed, because global zk is down");
        } catch (PulsarAdminException e) {
            // Ok
        }

        admin.clusters().createCluster("cluster-2", new ClusterData("test-url"));
    }
}
