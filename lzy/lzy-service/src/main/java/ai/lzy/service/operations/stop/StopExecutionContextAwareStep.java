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
    protected KafkaTopicDesc kafkaTopicDesc() {
        return state.kafkaTopicDesc;
    }

    protected void setKafkaTopicDesc(KafkaTopicDesc kafkaTopicDesc) {
        state.kafkaTopicDesc = kafkaTopicDesc;
    }
}
