package ai.lzy.servant.agents;

import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public enum AgentStatus {
    STARTED(0),
    REGISTERING(1),
    REGISTERED(2),
    PREPARING_EXECUTION(3),
    EXECUTING(4);

    private final int value;

    AgentStatus(int value) {
        this.value = value;
    }

    Servant.ServantStatus.Status toGrpcServantStatus() {
        return Servant.ServantStatus.Status.valueOf(name());
    }

    public int getValue() {
        return value;
    }
}
