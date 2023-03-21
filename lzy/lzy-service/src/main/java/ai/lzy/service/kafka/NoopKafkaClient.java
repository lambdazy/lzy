package ai.lzy.service.kafka;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "lzy-service.yc-kafka.enabled", notEquals = "true")
@Requires(property = "lzy-service.scram-kafka.enabled", notEquals = "true")
public class NoopKafkaClient implements KafkaClient {
    @Override
    public void createUser(String username, String password) {}

    @Override
    public void dropUser(String username) {}

    @Override
    public void createTopic(String name) {}

    @Override
    public void dropTopic(String name) {}

    @Override
    public void grantPermission(String username, String topicName, TopicRole role) {}

    @Override
    public void dropPermission(String username, String topicName, TopicRole role) {}

}
