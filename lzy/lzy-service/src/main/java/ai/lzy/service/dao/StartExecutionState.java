package ai.lzy.service.dao;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import jakarta.annotation.Nullable;

public final class StartExecutionState {
    @Nullable
    public KafkaTopicDesc kafkaTopicDesc;
    @Nullable
    public String allocatorSessionId;

    public static StartExecutionState initial() {
        return new StartExecutionState();
    }

    private StartExecutionState() {}

    public StartExecutionState(@Nullable KafkaTopicDesc kafkaTopicDesc, @Nullable String allocatorSessionId) {
        this.kafkaTopicDesc = kafkaTopicDesc;
        this.allocatorSessionId = allocatorSessionId;
    }
}
