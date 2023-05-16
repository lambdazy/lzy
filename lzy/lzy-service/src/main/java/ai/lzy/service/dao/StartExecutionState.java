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
}
