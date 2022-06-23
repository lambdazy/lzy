package ai.lzy.backoffice.grpc;

import ai.lzy.backoffice.models.auth.CheckPermissionRequest;
import ai.lzy.backoffice.models.auth.CheckSessionRequest;
import ai.lzy.backoffice.models.keys.AddPublicKeyRequest;
import ai.lzy.backoffice.models.keys.DeletePublicKeyRequest;
import ai.lzy.backoffice.models.keys.ListKeysRequest;
import ai.lzy.backoffice.models.tasks.GetTasksRequest;
import ai.lzy.backoffice.models.users.CreateUserRequest;
import ai.lzy.backoffice.models.users.DeleteUserRequest;
import ai.lzy.backoffice.models.users.ListUsersRequest;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ai.lzy.backoffice.configs.CredentialsProvider;
import ai.lzy.backoffice.configs.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;


@Singleton
public class Client {

    private final Channel channel;
    private final Channel wbApiChannel;

    @Inject
    CredentialsProvider credentials;

    @Inject
    Client(GrpcConfig config) {
        System.out.println("Starting channel on " + config.getHost() + ":" + config.getPort());

        channel = ChannelBuilder.forAddress(config.getHost(), config.getPort())
            .usePlaintext().enableRetry(LzyBackofficeGrpc.SERVICE_NAME).build();

        wbApiChannel = ChannelBuilder.forAddress(config.getWbhost(), config.getWbport())
            .usePlaintext().enableRetry(WbApiGrpc.SERVICE_NAME).build();
    }

    public LzyBackofficeGrpc.LzyBackofficeBlockingStub getBlockingStub() {
        return LzyBackofficeGrpc.newBlockingStub(channel);
    }

    public WbApiGrpc.WbApiBlockingStub getWbApiBlockingStub() {
        return WbApiGrpc.newBlockingStub(wbApiChannel);
    }

    public LzyBackofficeGrpc.LzyBackofficeStub getAsyncStub() {
        return LzyBackofficeGrpc.newStub(channel);
    }

    public BackOffice.AddKeyResult addToken(AddPublicKeyRequest request) {
        try {
            return getBlockingStub().addKey(
                request.toModel(credentials.createCreds())
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.CreateUserResult createUser(CreateUserRequest request) {
        try {
            return getBlockingStub().createUser(request.toModel(credentials.createCreds()));
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.DeleteUserResult deleteUser(DeleteUserRequest request) {
        try {
            return getBlockingStub().deleteUser(request.toModel(credentials.createCreds()));
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.ListUsersResponse listUsers(ListUsersRequest request) {
        try {
            return getBlockingStub().listUsers(request.toModel(credentials.createCreds()));
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    private HttpStatusException catchStatusException(StatusRuntimeException e) {
        switch (e.getStatus().getCode()) {
            case ALREADY_EXISTS:
            case OUT_OF_RANGE:
            case UNKNOWN:
            case INVALID_ARGUMENT:
                return new HttpStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
            case UNAUTHENTICATED:
            case UNAVAILABLE:
            case PERMISSION_DENIED:
                return new HttpStatusException(HttpStatus.FORBIDDEN, e.getMessage());
            case NOT_FOUND:
                return new HttpStatusException(HttpStatus.NOT_FOUND, e.getMessage());
            default:
                return new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public BackOffice.GenerateSessionIdResponse generateSessionId() {
        try {
            return getBlockingStub().generateSessionId(
                BackOffice.GenerateSessionIdRequest.newBuilder()
                    .setBackofficeCredentials(credentials.createCreds()).build()
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.CheckSessionResponse checkSession(CheckSessionRequest request) {
        try {
            return getBlockingStub().checkSession(
                request.toModel(credentials.createCreds())
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.AuthUserSessionResponse authUserSession(
        BackOffice.AuthUserSessionRequest.Builder request) {
        try {
            return getBlockingStub().authUserSession(
                request.setBackofficeCredentials(credentials.createCreds())
                    .build()
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.CheckPermissionResponse checkPermission(CheckPermissionRequest request) {
        try {
            return getBlockingStub().checkPermission(
                request.toModel(credentials.createCreds())
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.GetTasksResponse getTasks(GetTasksRequest request) {
        try {
            return getBlockingStub().getTasks(
                request.toModel(credentials.createCreds())
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.ListKeysResponse listTokens(ListKeysRequest request) {
        try {
            return getBlockingStub().listKeys(
                request.toModel(credentials.createCreds())
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }

    public BackOffice.DeleteKeyResponse deleteToken(DeletePublicKeyRequest request) {
        try {
            return getBlockingStub().deleteKey(
                request.toModel(credentials.createCreds())
            );
        } catch (StatusRuntimeException e) {
            throw catchStatusException(e);
        }
    }
}
