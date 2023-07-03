package ai.lzy.service.dao;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import jakarta.annotation.Nullable;

public final class StartExecutionState {
    @Nullable
    public KafkaTopicDesc kafkaTopicDesc;

    public static StartExecutionState initial() {
        return new StartExecutionState();
    }

    public static StartExecutionState of(@Nullable KafkaTopicDesc kafkaTopicDesc) {
        return new StartExecutionState(kafkaTopicDesc);
    }

    private StartExecutionState() {}

    public StartExecutionState(@Nullable KafkaTopicDesc kafkaTopicDesc) {
        this.kafkaTopicDesc = kafkaTopicDesc;
    }
}
