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

    public StopExecutionState(String finishPortalOpId, KafkaTopicDesc kafkaTopicDesc, String allocatorSessionId,
                              String deleteAllocSessionOpId, String portalSubjectId, String destroyChannelsOpId,
                              String portalVmId, String portalApiAddress)
    {
        this.finishPortalOpId = finishPortalOpId;
        this.kafkaTopicDesc = kafkaTopicDesc;
        this.allocatorSessionId = allocatorSessionId;
        this.deleteAllocSessionOpId = deleteAllocSessionOpId;
        this.portalSubjectId = portalSubjectId;
        this.destroyChannelsOpId = destroyChannelsOpId;
        this.portalVmId = portalVmId;
        this.portalApiAddress = portalApiAddress;
    }
}
