package com.yahoo.pulsar.broker.aspectj;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import com.yahoo.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;

public class ZookeeperClientFactoryImplTest {

    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    
    public static void main(String[] args) throws Exception{
        
        String serverList = "localhost:2181";
        
        ZooKeeperClientFactory zkFactory = new ZookeeperBkClientFactoryImpl();
        ZooKeeper zk = zkFactory.create(serverList, SessionType.ReadWrite, 30000).get();
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(2);
        zk.create("/test", "data".getBytes(), Acl, CreateMode.EPHEMERAL, (rc,  path,  ctx,  name) -> {
            System.out.println("ctx ="+ctx+", rc="+rc);
            latch.countDown();
        }, "create");
        zk.delete("/test", -1, ( rc,  path,  ctx) -> {
            System.out.println("ctx ="+ctx+", rc="+rc);
            latch2.countDown();
        }, "delete");
        latch.await();
        latch2.await();
        
    }
    
}
