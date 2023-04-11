package ai.lzy.util.kafka;

import io.grpc.StatusRuntimeException;

public interface KafkaAdminClient {

    void createUser(String username, String password) throws StatusRuntimeException;
    void dropUser(String username) throws StatusRuntimeException;

    void createTopic(String name) throws StatusRuntimeException;
    void dropTopic(String name) throws StatusRuntimeException;

    void grantPermission(String username, String topicName) throws StatusRuntimeException;

    void confirmAdminUser(String username, String requiredPassword) throws StatusRuntimeException;
}
