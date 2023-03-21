package ai.lzy.service.kafka;

import ai.lzy.service.config.LzyServiceConfig;
import com.google.protobuf.Int64Value;
import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.mdb.kafka.v1.*;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceGrpc.TopicServiceBlockingStub;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceOuterClass.CreateTopicRequest;
import yandex.cloud.api.mdb.kafka.v1.TopicServiceOuterClass.DeleteTopicRequest;
import yandex.cloud.api.mdb.kafka.v1.UserServiceGrpc.UserServiceBlockingStub;
import yandex.cloud.api.mdb.kafka.v1.UserServiceOuterClass.CreateUserRequest;
import yandex.cloud.api.mdb.kafka.v1.UserServiceOuterClass.GrantUserPermissionRequest;
import yandex.cloud.api.mdb.kafka.v1.UserServiceOuterClass.RevokeUserPermissionRequest;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;
import yandex.cloud.sdk.utils.OperationTimeoutException;
import yandex.cloud.sdk.utils.OperationUtils;

import java.nio.file.Path;
import java.time.Duration;

@Singleton
@Requires(property = "lzy-service.yc-kafka.enabled", value = "true")
public class YcKafkaClient implements KafkaClient {
    private static final Logger LOG = LogManager.getLogger(YcKafkaClient.class);
    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final  String clusterId;
    private final  UserServiceBlockingStub stub;
    private final  TopicServiceBlockingStub topicStub;
    private final  OperationServiceBlockingStub operationStub;

    public YcKafkaClient(LzyServiceConfig config) {
        var provider = Auth.apiKeyBuilder()
            .fromFile(Path.of(config.getYcKafka().getServiceAccountFile()))
            .cloudIAMEndpoint(config.getYcKafka().getIamEndpoint())
            .build();

        var factory = ServiceFactory.builder()
            .credentialProvider(provider)
            .endpoint(config.getYcKafka().getEndpoint())
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();

        stub = factory.create(UserServiceBlockingStub.class, UserServiceGrpc::newBlockingStub);
        operationStub = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
        topicStub = factory.create(TopicServiceBlockingStub.class, TopicServiceGrpc::newBlockingStub);

        clusterId = config.getYcKafka().getClusterId();
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
    public void grantPermission(String username, String topicName, TopicRole role) {

        var ycRole = switch (role) {
            case CONSUMER -> UserOuterClass.Permission.AccessRole.ACCESS_ROLE_CONSUMER;
            case PRODUCER -> UserOuterClass.Permission.AccessRole.ACCESS_ROLE_PRODUCER;
        };

        final OperationOuterClass.Operation op = stub.grantPermission(GrantUserPermissionRequest.newBuilder()
            .setClusterId(clusterId)
            .setUserName(username)
            .setPermission(UserOuterClass.Permission.newBuilder()
                .setTopicName(topicName)
                .setRole(ycRole)
                .build())
            .build());

        awaitOperation(op);
    }

    @Override
    public void dropPermission(String username, String topicName, TopicRole role) {
        var ycRole = switch (role) {
            case CONSUMER -> UserOuterClass.Permission.AccessRole.ACCESS_ROLE_CONSUMER;
            case PRODUCER -> UserOuterClass.Permission.AccessRole.ACCESS_ROLE_PRODUCER;
        };

        final OperationOuterClass.Operation op = stub.revokePermission(RevokeUserPermissionRequest.newBuilder()
            .setClusterId(clusterId)
            .setUserName(username)
            .setPermission(UserOuterClass.Permission.newBuilder()
                .setTopicName(topicName)
                .setRole(ycRole)
                .build())
            .build());

        awaitOperation(op);
    }

    private void awaitOperation(OperationOuterClass.Operation op) {
        OperationOuterClass.Operation readyOp;
        while (true) {
            try {
                readyOp = OperationUtils.wait(operationStub, op, Duration.ofSeconds(10));
                break;
            } catch (InterruptedException e) {
                // ignored
            } catch (OperationTimeoutException e) {
                LOG.error("Timeout exceeded", e);
                throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            }
        }

        if (readyOp.hasError()) {
            throw Status.fromCodeValue(readyOp.getError().getCode())
                .withDescription(readyOp.getError().getMessage())
                .asRuntimeException();
        }
    }
}
