package ai.lzy.service.operations.stop;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionContextAwareStep;
import ai.lzy.service.operations.ExecutionStepContext;
import jakarta.annotation.Nullable;

public abstract class StopExecutionContextAwareStep extends ExecutionContextAwareStep {
    private final StopExecutionState state;

    public StopExecutionContextAwareStep(ExecutionStepContext stepCtx, StopExecutionState state) {
        super(stepCtx);
        this.state = state;
    }

    protected StopExecutionState state() {
        return state;
    }

    @Nullable
    protected String finishPortalOpId() {
        return state.finishPortalOpId;
    }

    protected void setFinishPortalOpId(String opId) {
        state.finishPortalOpId = opId;
    }

    @Nullable
    protected KafkaTopicDesc kafkaTopicDesc() {
        return state.kafkaTopicDesc;
    }

    protected void setKafkaTopicDesc(KafkaTopicDesc kafkaTopicDesc) {
        state.kafkaTopicDesc = kafkaTopicDesc;
    }

    @Nullable
    protected String allocatorSessionId() {
        return state.allocatorSessionId;
    }

    protected void setAllocatorSessionId(String allocatorSessionId) {
        state.allocatorSessionId = allocatorSessionId;
    }

    @Nullable
    protected String deleteAllocSessionOpId() {
        return state.deleteAllocSessionOpId;
    }

    protected void setDeleteAllocSessionOpId(String opId) {
        state.deleteAllocSessionOpId = opId;
    }

    @Nullable
    protected String portalSubjectId() {
        return state.portalSubjectId;
    }

    protected void setPortalSubjectId(String portalSubjectId) {
        state.portalSubjectId = portalSubjectId;
    }

    @Nullable
    protected String destroyChannelsOpId() {
        return state.destroyChannelsOpId;
    }

    protected void setDestroyChannelsOpId(String opId) {
        state.destroyChannelsOpId = opId;
    }

    @Nullable
    protected String portalVmId() {
        return state.portalVmId;
    }

    protected void setPortalVmId(String portalVmId) {
        state.portalVmId = portalVmId;
    }

    @Nullable
    protected String portalApiAddress() {
        return state.portalApiAddress;
    }

    protected void setPortalApiAddress(String portalApiAddress) {
        state.portalApiAddress = portalApiAddress;
    }

}
