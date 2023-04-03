package ai.lzy.util.kafka;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static org.apache.kafka.common.acl.AclOperation.*;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

public class ScramKafkaAdminClient implements KafkaAdminClient {
    private static final Logger LOG = LogManager.getLogger(ScramKafkaAdminClient.class);

    private final AdminClient adminClient;

    public ScramKafkaAdminClient(KafkaConfig config) {
        var props = new KafkaHelper(config).toProperties();
        LOG.debug("Using SCRAM Kafka client with properties: {}", props);
        adminClient = AdminClient.create(props);
    }

    @Override
    public void createUser(String username, String password) throws StatusRuntimeException {
        try {
            adminClient.alterUserScramCredentials(
                List.of(
                    new UserScramCredentialUpsertion(
                        username,
                        new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096),
                        password)))
                .all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void dropUser(String username) throws StatusRuntimeException {
        try {
            adminClient.alterUserScramCredentials(
                List.of(new UserScramCredentialDeletion(username, ScramMechanism.SCRAM_SHA_512))).all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void createTopic(String name) throws StatusRuntimeException {
        try {
            // Do not do replicas and partitioning for now
            adminClient.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
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
            case PRODUCER -> WRITE;
            case CONSUMER -> READ;
        };

        try {
            adminClient.createAcls(
                List.of(
                    new AclBinding(
                        new ResourcePattern(TOPIC, topicName, LITERAL),
                        new AccessControlEntry("USER:" + username, "*", ALL, ALLOW)),
                    new AclBinding(
                        new ResourcePattern(CLUSTER, "kafka-cluster", LITERAL),
                        new AccessControlEntry("USER:" + username, "*", DESCRIBE, ALLOW))))
                .all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public void dropPermission(String username, String topicName, TopicRole role) throws StatusRuntimeException {
        var op = switch (role) {
            case PRODUCER -> WRITE;
            case CONSUMER -> READ;
        };

        try {
            adminClient.deleteAcls(
                List.of(
                    new AclBindingFilter(
                        new ResourcePatternFilter(TOPIC, topicName, LITERAL),
                        new AccessControlEntryFilter(username, "*", op, ALLOW))))
                .all().get();
        } catch (Exception e) {
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }
}
