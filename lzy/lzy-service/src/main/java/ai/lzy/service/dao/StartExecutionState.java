package ai.lzy.service.dao;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import jakarta.annotation.Nullable;

public final class StartExecutionState {
    @Nullable
    public KafkaTopicDesc kafkaTopicDesc;
    @Nullable
    public String allocatorSessionId;
    @Nullable
    public String portalId;
    @Nullable
    public String portalSubjectId;
    @Nullable
    public String allocateVmOpId;
    @Nullable
    public String portalVmId;
    @Nullable
    public String portalApiAddress;

    public static StartExecutionState initial() {
        return new StartExecutionState();
    }

    public static StartExecutionState of(@Nullable KafkaTopicDesc kafkaTopicDesc, @Nullable String allocatorSessionId,
                                         @Nullable String portalId, @Nullable String portalSubjectId,
                                         @Nullable String portalVmId, @Nullable String portalApiAddress)
    {
        return new StartExecutionState(kafkaTopicDesc, allocatorSessionId, portalId, portalSubjectId, null, portalVmId,
            portalApiAddress);
    }

    private StartExecutionState() {}

    public StartExecutionState(@Nullable KafkaTopicDesc kafkaTopicDesc, @Nullable String allocatorSessionId,
                               @Nullable String portalId, @Nullable String portalSubjectId,
                               @Nullable String allocateVmOpId, @Nullable String portalVmId,
                               @Nullable String portalApiAddress)
    {
        this.kafkaTopicDesc = kafkaTopicDesc;
        this.allocatorSessionId = allocatorSessionId;
        this.portalId = portalId;
        this.portalSubjectId = portalSubjectId;
        this.allocateVmOpId = allocateVmOpId;
        this.portalVmId = portalVmId;
        this.portalApiAddress = portalApiAddress;
    }
}
