package ai.lzy.util.kafka;

import io.grpc.StatusRuntimeException;

public class NoopKafkaAdminClient implements KafkaAdminClient {
    @Override
    public void createUser(String username, String password) {}

    @Override
    public void dropUser(String username) {}

    @Override
    public void createTopic(String name) {}

    @Override
    public void dropTopic(String name) {}

    @Override
    public void grantPermission(String username, String topicName) {}

    @Override
    public void confirmAdminUser(String username, String requiredPassword) throws StatusRuntimeException {}
}
