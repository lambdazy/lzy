package ai.lzy.service.kafka;

import ai.lzy.service.config.LzyServiceConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.List;

@Singleton
@Requires(property = "lzy-service.scram-kafka.enabled", value = "true")
public class ScramKafkaClient implements KafkaClient {
    private final AdminClient adminClient;

    public ScramKafkaClient(LzyServiceConfig config) {
        var credentials = config.getScramKafka();

        var props = config.getKafka().helper()
            .withCredentials(config.getKafka().getBootstrapServers(),
                credentials.getUsername(), credentials.getPassword())
            .props();

        assert props != null;
        adminClient = AdminClient.create(props);

    }

    @Override
    public void createUser(String username, String password) throws StatusRuntimeException {
        try {
            adminClient.alterUserScramCredentials(List.of(
                new UserScramCredentialUpsertion(
                    username,
                    new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096),
                    password)
            )).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void dropUser(String username) throws StatusRuntimeException {
        try {
            adminClient.alterUserScramCredentials(List.of(
                new UserScramCredentialDeletion(username, ScramMechanism.SCRAM_SHA_512)
            )).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void createTopic(String name) throws StatusRuntimeException {
        try {
            adminClient.createTopics(List.of(
                new NewTopic(name, 1, (short) 1)  // Do not do replicas and partitioning for now
            )).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void dropTopic(String name) throws StatusRuntimeException {
        try {
            adminClient.deleteTopics(List.of(name)).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void grantPermission(String username, String topicName, TopicRole role) throws StatusRuntimeException {

        var op = switch (role) {
            case PRODUCER -> AclOperation.WRITE;
            case CONSUMER -> AclOperation.READ;
        };

        try {
            adminClient.createAcls(List.of(
                new AclBinding(
                    new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                    new AccessControlEntry("USER:" + username, "*", op, AclPermissionType.ALLOW)
                )
            )).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void dropPermission(String username, String topicName, TopicRole role) throws StatusRuntimeException {
        var op = switch (role) {
            case PRODUCER -> AclOperation.WRITE;
            case CONSUMER -> AclOperation.READ;
        };

        try {
            adminClient.deleteAcls(List.of(
                new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                    new AccessControlEntryFilter(username, "*", op, AclPermissionType.ALLOW)
                )
            )).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }
}
