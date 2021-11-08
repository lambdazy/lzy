package ru.yandex.cloud.ml.platform.lzy.backoffice.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.*;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;


@Singleton
public class Client {
    private final Channel channel;

    @Inject
    CredentialsConfig credentials;

    @Inject
    Client(GrpcConfig config){
        System.out.println("Starting channel on "+ config.getHost() + ":" + config.getPort());

        channel = ManagedChannelBuilder.forAddress(config.getHost(), config.getPort()).usePlaintext().build();
    }

    public LzyBackofficeGrpc.LzyBackofficeBlockingStub getBlockingStub(){
        return LzyBackofficeGrpc.newBlockingStub(channel);
    }

    public LzyBackofficeGrpc.LzyBackofficeStub getAsyncStub(){
        return LzyBackofficeGrpc.newStub(channel);
    }

    public BackOffice.AddTokenResult addToken(AddTokenRequest request){
        try {
            return getBlockingStub().addToken(
                    request.toModel(credentials.getCredentials())
            );
        }
        catch (StatusRuntimeException e){
            throw catchStatusException(e);
        }
    }

    public BackOffice.CreateUserResult createUser(CreateUserRequest request){
        try {
            return getBlockingStub().createUser(request.toModel(credentials.getCredentials()));
        }
        catch (StatusRuntimeException e){
            throw catchStatusException(e);
        }
    }

    public BackOffice.DeleteUserResult deleteUser(DeleteUserRequest request){
        try {
            return getBlockingStub().deleteUser(request.toModel(credentials.getCredentials()));
        }
        catch (StatusRuntimeException e){
            throw catchStatusException(e);
        }
    }
    public BackOffice.ListUsersResponse listUsers(ListUsersRequest request){
        try {
            return getBlockingStub().listUsers(request.toModel(credentials.getCredentials()));
        }
        catch (StatusRuntimeException e){
            throw catchStatusException(e);
        }
    }

    private HttpStatusException catchStatusException(StatusRuntimeException e){
        switch (e.getStatus().getCode()){
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
}
