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
package org.apache.pulsar.common.policies.data;

import com.google.common.base.Objects;

public class PersistencePolicies {
    private int bookkeeperEnsemble;
    private int bookkeeperWriteQuorum;
    private int bookkeeperAckQuorum;

    public PersistencePolicies() {
        this(2, 2, 2);
    }

    public PersistencePolicies(int bookkeeperEnsemble, int bookkeeperWriteQuorum, int bookkeeperAckQuorum) {
        this.bookkeeperEnsemble = bookkeeperEnsemble;
        this.bookkeeperWriteQuorum = bookkeeperWriteQuorum;
        this.bookkeeperAckQuorum = bookkeeperAckQuorum;
    }

    public int getBookkeeperEnsemble() {
        return bookkeeperEnsemble;
    }

    public int getBookkeeperWriteQuorum() {
        return bookkeeperWriteQuorum;
    }

    public int getBookkeeperAckQuorum() {
        return bookkeeperAckQuorum;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PersistencePolicies) {
            PersistencePolicies other = (PersistencePolicies) obj;
            return bookkeeperEnsemble == other.bookkeeperEnsemble
                    && bookkeeperWriteQuorum == other.bookkeeperWriteQuorum
                    && bookkeeperAckQuorum == other.bookkeeperAckQuorum;
        }

        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("e", bookkeeperEnsemble).add("w", bookkeeperWriteQuorum)
                .add("a", bookkeeperAckQuorum).toString();
    }
}
