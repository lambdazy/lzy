package ru.yandex.cloud.ml.platform.lzy.backoffice.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;


@Singleton
public class Client {
    private final Channel channel;

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

    public BackOffice.AddUserResult addUser(BackOffice.User user){
        return getBlockingStub().addUser(user);
    }
}
