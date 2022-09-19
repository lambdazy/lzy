package ru.yandex.cloud.ml.platform.model.util.election;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class DistributedLeaderElectionTest {

    private TestingCluster zkServer;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingCluster(3);
        zkServer.start();
    }

    @After
    public void tearDown() throws Exception {
        zkServer.close();
    }

    @Test
    public void testLeaderElection() {
        final CuratorFramework[] curators = new CuratorFramework[] {
            getClient(zkServer.getConnectString()),
            getClient(zkServer.getConnectString()),
            getClient(zkServer.getConnectString())
        };

        final LeaderElection[] elections = Arrays.stream(curators)
            .map(curatorFramework -> new DistributedLeaderElection(curatorFramework, "prefix"))
            .toArray(LeaderElection[]::new);

        int leadersCount = 0;
        int leaderIndex = 0;
        for (int i = 0; i < elections.length; i++) {
            if (elections[i].isLeader()) {
                leadersCount++;
                leaderIndex = i;
            }
        }
        //there must be a single leader
        Assert.assertEquals(1, leadersCount);

        curators[leaderIndex].close();
        leadersCount = 0;
        while (leadersCount == 0) {
            for (int i = 0; i < elections.length; i++) {
                if (i != leaderIndex && elections[i].isLeader()) {
                    leadersCount++;
                }
            }
        }
        //there must be a single leader
        Assert.assertEquals(1, leadersCount);

        Arrays.stream(curators).forEach(CuratorFramework::close);
    }

    private CuratorFramework getClient(String connectString) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return client;
    }
}
