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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is a simple holder of the client configuration values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClientConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private String serviceUrl;
    @JsonIgnore
    private transient ServiceUrlProvider serviceUrlProvider;

    @JsonIgnore
    private transient Authentication authentication = new AuthenticationDisabled();
    @JsonIgnore
    private transient String authPluginClassName;
    @JsonIgnore
    private transient Map<String, String> authParams;

    private long operationTimeoutMs = 30000;
    private long statsIntervalSeconds = 60;

    private int numIoThreads = 1;
    private int numListenerThreads = 1;
    private int connectionsPerBroker = 1;

    private boolean useTcpNoDelay = true;

    private boolean useTls = false;
    private String tlsTrustCertsFilePath = "";
    private boolean tlsAllowInsecureConnection = false;
    private boolean tlsHostnameVerificationEnable = false;
    private int concurrentLookupRequest = 5000;
    private int maxLookupRequest = 50000;
    private int maxNumberOfRejectedRequestPerConnection = 50;
    private int maxChunkedMessagesBuffers = 0;
    private int keepAliveIntervalSeconds = 30;
    private int connectionTimeoutMs = 10000;
    private int requestTimeoutMs = 60000;
    private long defaultBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
    private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(30);

    public ClientConfigurationData clone() {
        try {
            return (ClientConfigurationData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientConfigurationData");
        }
    }


}
