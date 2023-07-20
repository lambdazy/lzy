package ai.lzy.service.dao;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import jakarta.annotation.Nullable;

public final class StopExecutionState {
    @Nullable
    public KafkaTopicDesc kafkaTopicDesc;

    public StopExecutionState() {}

    public StopExecutionState(KafkaTopicDesc kafkaTopicDesc) {
        this.kafkaTopicDesc = kafkaTopicDesc;
    }
}
