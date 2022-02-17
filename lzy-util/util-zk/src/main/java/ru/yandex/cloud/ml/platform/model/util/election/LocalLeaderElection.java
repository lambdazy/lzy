package ru.yandex.cloud.ml.platform.model.util.election;

import org.springframework.stereotype.Service;

@Service("LocalLeaderElection")
public class LocalLeaderElection implements LeaderElection {

    @Override
    public boolean isLeader() {
        return true;
    }
}
