package ru.yandex.cloud.ml.platform.model.util.election;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Lazy
@Service("DistributedLeaderElection")
public class DistributedLeaderElection implements LeaderElection {

    private final LeaderLatch leaderLatch;

    @Autowired
    public DistributedLeaderElection(
        CuratorFramework curatorFramework,
        @Value("${leader.election.prefix}") String prefix
    ) {
        leaderLatch = new LeaderLatch(curatorFramework, "/" + prefix);
        try {
            leaderLatch.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isLeader() {
        try {
            while (!leaderLatch.getLeader().isLeader()) {
                Thread.sleep(1000);
            }
            return leaderLatch.hasLeadership();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
