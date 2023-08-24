package ai.lzy.util.kafka;

import com.google.protobuf.Int64Value;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.mdb.kafka.v1.TopicOuterClass;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceGrpc;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceGrpc.TopicServiceBlockingStub;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceOuterClass.CreateTopicRequest;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceOuterClass.DeleteTopicRequest;
import yandex.cloud.api.mdb.kafka.v1.UserOuterClass;
import yandex.cloud.api.mdb.kafka.v1.UserServiceGrpc;
import yandex.cloud.api.mdb.kafka.v1.UserServiceGrpc.UserServiceBlockingStub;
import yandex.cloud.api.mdb.kafka.v1.UserServiceOuterClass;
import yandex.cloud.api.mdb.kafka.v1.UserServiceOuterClass.CreateUserRequest;
import yandex.cloud.api.mdb.kafka.v1.UserServiceOuterClass.GrantUserPermissionRequest;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.api.operation.OperationServiceOuterClass.CancelOperationRequest;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;
import yandex.cloud.sdk.utils.OperationTimeoutException;
import yandex.cloud.sdk.utils.OperationUtils;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

public class YcKafkaAdminClient implements KafkaAdminClient {
    private static final Logger LOG = LogManager.getLogger(YcKafkaAdminClient.class);
    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final String clusterId;
    private final UserServiceBlockingStub stub;
    private final TopicServiceBlockingStub topicStub;
    private final OperationServiceBlockingStub operationStub;

    public YcKafkaAdminClient(String endpoint, String clusterId, String serviceAccountFile, String iamEndpoint) {
        var provider = Auth.apiKeyBuilder()
            .fromFile(Path.of(serviceAccountFile))
            .cloudIAMEndpoint(iamEndpoint)
            .build();

        var factory = ServiceFactory.builder()
            .credentialProvider(provider)
            .endpoint(endpoint)
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();

        stub = factory.create(UserServiceBlockingStub.class, UserServiceGrpc::newBlockingStub);
        operationStub = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        topicStub = factory.create(TopicServiceBlockingStub.class, TopicServiceGrpc::newBlockingStub);

        this.clusterId = clusterId;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void createUser(String username, String password) {
        final OperationOuterClass.Operation op = stub.create(CreateUserRequest.newBuilder()
            .setClusterId(clusterId)
            .setUserSpec(UserOuterClass.UserSpec.newBuilder()
                .setName(username)
                .setPassword(password)
                .build())
            .build());

        awaitOperation(op);
    }

    @Override
    public void dropUser(String username) {
        final OperationOuterClass.Operation op = stub.delete(UserServiceOuterClass.DeleteUserRequest.newBuilder()
            .setClusterId(clusterId)
            .setUserName(username)
            .build());

        awaitOperation(op);
    }

    @Override
    public void createTopic(String name) {
        var op = topicStub.create(CreateTopicRequest.newBuilder()
            .setClusterId(clusterId)
            .setTopicSpec(TopicOuterClass.TopicSpec.newBuilder()
                .setName(name)
                .setPartitions(Int64Value.of(1))
                .setReplicationFactor(Int64Value.of(1))
                .build())
            .build());

        awaitOperation(op);
    }

    @Override
    public void dropTopic(String name) {
        var op = topicStub.delete(DeleteTopicRequest.newBuilder()
            .setClusterId(clusterId)
            .setTopicName(name)
            .build());

        awaitOperation(op);
    }

    @Override
    public void grantPermission(String username, String topicName) {
        final OperationOuterClass.Operation op = stub.grantPermission(GrantUserPermissionRequest.newBuilder()
            .setClusterId(clusterId)
            .setUserName(username)
            .setPermission(UserOuterClass.Permission.newBuilder()
                .setTopicName(topicName)
                .setRole(UserOuterClass.Permission.AccessRole.ACCESS_ROLE_ADMIN)
                .build())
            .build());

        awaitOperation(op);
    }

    private void awaitOperation(OperationOuterClass.Operation op) {
        OperationOuterClass.Operation readyOp;

        var start = Instant.now();
        while (true) {
            try {
                readyOp = OperationUtils.wait(operationStub, op, Duration.ofSeconds(60));
                break;
            } catch (InterruptedException e) {
                // ignored
            } catch (OperationTimeoutException e) {
                LOG.error("Timeout exceeded", e);
                try {
                    operationStub.cancel(CancelOperationRequest.newBuilder()
                        .setOperationId(op.getId())
                        .build());
                } catch (Exception ex) {
                    LOG.error("Error while cancelling operation by timeout", ex);
                }
                throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            }
        }

        LOG.info("Operation completed in {} ms", Duration.between(start, Instant.now()).toMillis());

        if (readyOp.hasError()) {
            throw Status.fromCodeValue(readyOp.getError().getCode())
                .withDescription(readyOp.getError().getMessage())
                .asRuntimeException();
        }
    }
}
