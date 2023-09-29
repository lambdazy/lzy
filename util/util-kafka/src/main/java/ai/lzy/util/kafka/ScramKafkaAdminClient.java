package ai.lzy.util.kafka;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
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
    public void shutdown() {
        adminClient.close();
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
            LOG.error("Error while creating user {}", username, e);
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
    public void grantPermission(String username, String topicName) throws StatusRuntimeException {
        try {
            adminClient.createAcls(
                List.of(
                    new AclBinding(
                        new ResourcePattern(TOPIC, topicName, LITERAL),
                        new AccessControlEntry("User:" + username, "*", ALL, ALLOW)),
                    new AclBinding(
                        new ResourcePattern(TOPIC, topicName, LITERAL),
                        new AccessControlEntry("User:" + username, "*", DESCRIBE, ALLOW))))
                .all().get();
        } catch (Exception e) {
            LOG.error("Cannot grant permission: ", e);
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }

    @Override
    public boolean isTopicExists(String name) throws StatusRuntimeException {
        var future = adminClient.describeTopics(List.of(name))
            .topicNameValues()
            .get(name);

        try {
            var descr = future.get();
            LOG.debug("Topic {}: {}", name, descr.toString());
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                LOG.debug("Topic {} doesnt exist", name);
                return false;
            }
            LOG.error("Cannot describe topic {}: ", name, e);
            throw Status.fromThrowable(e).asRuntimeException();
        } catch (Exception e) {
            LOG.error("Cannot describe topic {}: ", name, e);
            throw Status.fromThrowable(e).asRuntimeException();
        }
    }
}
