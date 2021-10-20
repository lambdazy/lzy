package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public enum AgentStatus {
    STARTED,
    REGISTERING,
    REGISTERED,
    PREPARING_EXECUTION,
    EXECUTING;

    Servant.ServantStatus.Status toGrpcServantStatus() {
        return Servant.ServantStatus.Status.valueOf(name());
    }
}
