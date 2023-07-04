package ai.lzy.service.operations.start;

import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionContextAwareStep;
import ai.lzy.service.operations.ExecutionStepContext;
import jakarta.annotation.Nullable;

import static ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;

abstract class StartExecutionContextAwareStep extends ExecutionContextAwareStep {
    private final StartExecutionState state;

    protected StartExecutionContextAwareStep(ExecutionStepContext stepCtx, StartExecutionState state) {
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

    @Nullable
    protected String allocatorSessionId() {
        return state.allocatorSessionId;
    }

    protected void setAllocatorSessionId(String allocatorSessionId) {
        state.allocatorSessionId = allocatorSessionId;
    }

    protected void setKafkaTopicDesc(KafkaTopicDesc kafkaTopicDesc) {
        state.kafkaTopicDesc = kafkaTopicDesc;
    }
}
