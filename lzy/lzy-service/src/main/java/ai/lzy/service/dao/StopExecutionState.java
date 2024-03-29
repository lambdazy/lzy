package ai.lzy.service.dao;

import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import jakarta.annotation.Nullable;

public final class StopExecutionState {
    @Nullable
    public KafkaTopicDesc kafkaTopicDesc;
    @Nullable
    public String allocatorSessionId;

    public StopExecutionState() {}

    public StopExecutionState(KafkaTopicDesc kafkaTopicDesc, String allocatorSessionId) {
        this.kafkaTopicDesc = kafkaTopicDesc;
        this.allocatorSessionId = allocatorSessionId;
    }
}
