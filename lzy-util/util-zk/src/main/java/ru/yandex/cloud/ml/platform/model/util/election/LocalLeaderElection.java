package ru.yandex.cloud.ml.platform.model.util.election;

public class LocalLeaderElection implements LeaderElection {

    @Override
    public boolean isLeader() {
        return true;
    }
}
