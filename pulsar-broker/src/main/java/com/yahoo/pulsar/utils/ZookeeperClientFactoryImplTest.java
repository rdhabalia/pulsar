package com.yahoo.pulsar.utils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

public class ZookeeperClientFactoryImplTest {

    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    
    
    public static void main(String[] args) 
    {
        try {
            String serverList = "localhost:2181";
            ZooKeeperClientFactory zkfactory = new ZookeeperClientFactoryImpl();
            ZooKeeper zk = zkfactory.create(serverList, SessionType.ReadWrite, 30000).get();
            CountDownLatch latch = new CountDownLatch(1);
            zk.create("/test", "data".getBytes(), Acl, CreateMode.EPHEMERAL, (rc,  path,  ctx,  name) -> {
                System.out.println("ctx ="+ctx);
                latch.countDown();
            }, "test");
            latch.await();
        }catch(Exception e) {
            e.printStackTrace();
        }
        
    }
    
}
