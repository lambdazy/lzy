package ai.lzy.service.dao;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import jakarta.annotation.Nullable;

public final class StopExecutionState {
    @Nullable
    public String finishPortalOpId;
    @Nullable
    public KafkaTopicDesc kafkaTopicDesc;
    @Nullable
    public String allocatorSessionId;
    @Nullable
    public String deleteAllocSessionOpId;
    @Nullable
    public String portalSubjectId;
    @Nullable
    public String destroyChannelsOpId;
    @Nullable
    public String portalVmId;
    @Nullable
    public String portalApiAddress;
}
