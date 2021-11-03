package ru.yandex.cloud.ml.platform.lzy.backoffice.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
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
        return getBlockingStub().addToken(
                request.toModel(credentials.getCredentials())
        );
    }

    public BackOffice.CreateUserResult createUser(CreateUserRequest request){
        return getBlockingStub().createUser(request.toModel(credentials.getCredentials()));
    }

    public BackOffice.DeleteUserResult deleteUser(DeleteUserRequest request){
        return getBlockingStub().deleteUser(request.toModel(credentials.getCredentials()));
    }
    public BackOffice.ListUsersResponse listUsers(ListUsersRequest request){
        return getBlockingStub().listUsers(request.toModel(credentials.getCredentials()));
    }
}
