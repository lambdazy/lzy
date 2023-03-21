package ai.lzy.service.kafka;

import io.grpc.StatusRuntimeException;

public interface KafkaClient {
    enum TopicRole {
        PRODUCER,
        CONSUMER
    }

    void createUser(String username, String password) throws StatusRuntimeException;
    void dropUser(String username) throws StatusRuntimeException;

    void createTopic(String name) throws StatusRuntimeException;
    void dropTopic(String name) throws StatusRuntimeException;

    void grantPermission(String username, String topicName, TopicRole role) throws StatusRuntimeException;
    void dropPermission(String username, String topicName, TopicRole role) throws StatusRuntimeException;

}
