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
package org.apache.pulsar.common.naming;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import org.apache.pulsar.common.naming.DestinationDomain;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.annotations.Test;

@Test
public class NamespaceNameTest {

    @Test
    void namespace() {
        try {
            NamespaceName.get("namespace");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NamespaceName.get("property.namespace");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NamespaceName.get("0.0.0.0");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NamespaceName.get("property.namespace:destination");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("property/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("property/cluster/namespace/destination");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get(null);
        } catch (IllegalArgumentException e) {
            // OK
        }

        try {
            NamespaceName.get(null, "use", "ns1");
        } catch (IllegalArgumentException e) {
            // OK
        }

        assertEquals(NamespaceName.get("prop/cluster/ns").getPersistentTopicName("ds"),
                "persistent://prop/cluster/ns/ds");

        try {
            NamespaceName.get("prop/cluster/ns").getDestinationName(null, "ds");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        assertEquals(NamespaceName.get("prop/cluster/ns").getDestinationName(DestinationDomain.persistent, "ds"),
                "persistent://prop/cluster/ns/ds");
        assertEquals(NamespaceName.get("prop/cluster/ns"), NamespaceName.get("prop/cluster/ns"));
        assertEquals(NamespaceName.get("prop/cluster/ns").toString(), "prop/cluster/ns");
        assertFalse(NamespaceName.get("prop/cluster/ns").equals("prop/cluster/ns"));

        assertEquals(NamespaceName.get("prop", "cluster", "ns"), NamespaceName.get("prop/cluster/ns"));
        assertEquals(NamespaceName.get("prop/cluster/ns").getProperty(), "prop");
        assertEquals(NamespaceName.get("prop/cluster/ns").getCluster(), "cluster");
        assertEquals(NamespaceName.get("prop/cluster/ns").getLocalName(), "ns");

        try {
            NamespaceName.get("ns").getProperty();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("ns").getCluster();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("ns").getLocalName();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("_pulsar/cluster/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get(null, "cluster", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("", "cluster", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("/cluster/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar//namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", null, "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", "", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar/cluster/");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", "cluster", null);
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", "cluster", "");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        NamespaceName v2Namespace = NamespaceName.get("pulsar/colo1/testns-1");
        assertEquals(v2Namespace.getProperty(), "pulsar");
        assertEquals(v2Namespace.getCluster(), "colo1");
        assertEquals(v2Namespace.getLocalName(), "testns-1");
    }
}
