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
package org.apache.pulsar.common.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.testng.annotations.Test;

public class PulsarConfigurationLoaderTest {

    @Test
    public void testPulsarConfiguraitonLoadingStream() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String zkServer = "z1.example.com,z2.example.com,z3.example.com";
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=" + zkServer);
        printWriter.println("globalZookeeperServers=gz1.example.com,gz2.example.com,gz3.example.com/foo");
        printWriter.println("brokerDeleteInactiveTopicsEnabled=true");
        printWriter.println("statusFilePath=/tmp/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("backlogQuotaDefaultLimitGB=18");
        printWriter.println("clusterName=usc");
        printWriter.println("brokerClientAuthenticationPlugin=test.xyz.client.auth.plugin");
        printWriter.println("brokerClientAuthenticationParameters=role:my-role");
        printWriter.println("superUserRoles=appid1,appid2");
        printWriter.println("brokerServicePort=7777");
        printWriter.println("managedLedgerDefaultMarkDeleteRateLimit=5.0");
        printWriter.close();
        testConfigFile.deleteOnExit();
        InputStream stream = new FileInputStream(testConfigFile);
    }

}
