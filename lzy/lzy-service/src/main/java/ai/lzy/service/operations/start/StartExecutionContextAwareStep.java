package ai.lzy.service.operations.start;

import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionContextAwareStep;
import ai.lzy.service.operations.ExecutionStepContext;
import jakarta.annotation.Nullable;

import static ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;

public abstract class StartExecutionContextAwareStep extends ExecutionContextAwareStep {
    private final StartExecutionState state;

    public StartExecutionContextAwareStep(ExecutionStepContext stepCtx, StartExecutionState state) {
        super(stepCtx);
        this.state = state;
    }

    protected StartExecutionState state() {
        return state;
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
    protected String portalId() {
        return state.portalId;
    }

    protected void setPortalId(String portalId) {
        state.portalId = portalId;
    }

    @Nullable
    protected String portalSubjectId() {
        return state.portalSubjectId;
    }

    protected void setPortalSubjectId(String portalSubjectId) {
        state.portalSubjectId = portalSubjectId;
    }

    @Nullable
    protected String allocateVmOpId() {
        return state.allocateVmOpId;
    }

    protected void setAllocateVmOpId(String allocateVmOpId) {
        state.allocateVmOpId = allocateVmOpId;
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
