package ru.yandex.cloud.ml.platform.model.util.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class DistributedLockManagerTest extends LockManagerBaseTest {

    private TestingCluster zkServer;
    private CuratorFramework curatorClient;
    private DistributedLockManager lockManager;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingCluster(3);
        zkServer.start();

        curatorClient = CuratorFrameworkFactory.newClient(
            zkServer.getConnectString(),
            new ExponentialBackoffRetry(1000, 3)
        );
        curatorClient.start();

        lockManager = new DistributedLockManager(curatorClient, 10);
    }

    @After
    public void tearDown() throws IOException {
        curatorClient.close();
        zkServer.close();
    }

    @Override
    LockManager lockManager() {
        return lockManager;
    }
}
