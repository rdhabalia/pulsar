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
package com.yahoo.pulsar.broker;

import java.io.IOException;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.IsClosedCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleZKBookKeeperClientFactoryImpl implements BookKeeperClientFactory {

    private PulsarService pulsar;
    private static final String readOnlyLedgerIdLimitZkPath = "/admin/readOnlyLedgerIdLimit";

    public MultipleZKBookKeeperClientFactoryImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient) throws IOException {

        BookKeeperClientFactoryImpl defaultFactory = new BookKeeperClientFactoryImpl();
        ZooKeeper readOnlyZkClient = this.pulsar.getZkClient();
        BookKeeper readOnlyBkClient = defaultFactory.create(conf, readOnlyZkClient);
        long readOnlyLedgerIdLimit = getReadOnlyLedgerIdLimit(readOnlyZkClient);
        try {
            return new BookKeeperWraper(defaultFactory.createBookkeeperClientConfiguration(conf, zkClient), zkClient,
                    readOnlyLedgerIdLimit, readOnlyBkClient);
        } catch (InterruptedException | KeeperException e) {
            throw new IOException(e);
        }
    }

    private long getReadOnlyLedgerIdLimit(ZooKeeper localZkClient) throws IOException {
        try {
            return Long.valueOf(new String(localZkClient.getData(readOnlyLedgerIdLimitZkPath, null, null))).longValue();
        } catch (Exception e) {
            log.error("Failed to get data from {}", readOnlyLedgerIdLimitZkPath, e);
            throw new IOException(e);
        }
    }

    static class BookKeeperWraper extends BookKeeper {

        private long readOnlyLedgerIdLimit;
        private BookKeeper readOnlyBkClient;

        public BookKeeperWraper(ClientConfiguration bkConf, ZooKeeper zkClient, long readOnlyLedgerIdLimit,
                BookKeeper readOnlyBkClient) throws IOException, InterruptedException, KeeperException {
            super(bkConf, zkClient);
            this.readOnlyLedgerIdLimit = readOnlyLedgerIdLimit;
            this.readOnlyBkClient = readOnlyBkClient;
        }

        public void asyncCreateLedger(int ensSize, int writeQuorumSize, BookKeeper.DigestType digestType, byte[] passwd,
                CreateCallback cb, Object ctx) {
            super.asyncCreateLedger(ensSize, writeQuorumSize, digestType, passwd, cb, ctx);

        }

        public void asyncCreateLedger(int ensSize, int writeQuorumSize, int ackQuorumSize,
                BookKeeper.DigestType digestType, byte[] passwd, CreateCallback cb, Object ctx) {
            super.asyncCreateLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, cb, ctx);
        }

        public LedgerHandle createLedger(BookKeeper.DigestType digestType, byte[] passwd)
                throws BKException, InterruptedException {
            return super.createLedger(digestType, passwd);
        }

        public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize,
                BookKeeper.DigestType digestType, byte[] passwd) throws InterruptedException, BKException {
            return super.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
        }

        public void asyncOpenLedger(long ledgerId, BookKeeper.DigestType digestType, byte[] passwd, OpenCallback cb,
                Object ctx) {
            if (ownedByReadOnlyBk(ledgerId)) {
                readOnlyBkClient.asyncOpenLedger(ledgerId, digestType, passwd, cb, ctx);
            } else {
                super.asyncOpenLedger(ledgerId, digestType, passwd, cb, ctx);
            }
        }

        public void asyncOpenLedgerNoRecovery(long ledgerId, BookKeeper.DigestType digestType, byte[] passwd,
                OpenCallback cb, Object ctx) {
            if (ownedByReadOnlyBk(ledgerId)) {
                readOnlyBkClient.asyncOpenLedgerNoRecovery(ledgerId, digestType, passwd, cb, ctx);
            } else {
                super.asyncOpenLedgerNoRecovery(ledgerId, digestType, passwd, cb, ctx);
            }
        }

        public LedgerHandle openLedger(long ledgerId, BookKeeper.DigestType digestType, byte[] passwd)
                throws BKException, InterruptedException {
            if (ownedByReadOnlyBk(ledgerId)) {
                return readOnlyBkClient.openLedger(ledgerId, digestType, passwd);
            } else {
                return super.openLedger(ledgerId, digestType, passwd);
            }
        }

        public LedgerHandle openLedgerNoRecovery(long ledgerId, BookKeeper.DigestType digestType, byte[] passwd)
                throws BKException, InterruptedException {
            if (ownedByReadOnlyBk(ledgerId)) {
                return readOnlyBkClient.openLedgerNoRecovery(ledgerId, digestType, passwd);
            } else {
                return super.openLedgerNoRecovery(ledgerId, digestType, passwd);
            }
        }

        public void asyncDeleteLedger(long ledgerId, DeleteCallback cb, Object ctx) {
            if (ownedByReadOnlyBk(ledgerId)) {
                readOnlyBkClient.asyncDeleteLedger(ledgerId, cb, ctx);
            } else {
                super.asyncDeleteLedger(ledgerId, cb, ctx);
            }
        }

        public void deleteLedger(long ledgerId) throws InterruptedException, BKException {
            if (ownedByReadOnlyBk(ledgerId)) {
                readOnlyBkClient.deleteLedger(ledgerId);
            } else {
                super.deleteLedger(ledgerId);
            }
        }

        public void asyncIsClosed(long ledgerId, final IsClosedCallback cb, final Object ctx) {
            if (ownedByReadOnlyBk(ledgerId)) {
                readOnlyBkClient.asyncIsClosed(ledgerId, cb, ctx);
            } else {
                super.asyncIsClosed(ledgerId, cb, ctx);
            }
        }

        public boolean isClosed(long ledgerId) throws BKException, InterruptedException {
            if (ownedByReadOnlyBk(ledgerId)) {
                return readOnlyBkClient.isClosed(ledgerId);
            } else {
                return super.isClosed(ledgerId);
            }
        }

        public void close() throws InterruptedException, BKException {
            super.close();
            readOnlyBkClient.close();
        }

        private boolean ownedByReadOnlyBk(long ledgerId) {
            return ledgerId < readOnlyLedgerIdLimit;
        }

    }
    
    private static final Logger log = LoggerFactory.getLogger(MultipleZKBookKeeperClientFactoryImpl.class);
}
