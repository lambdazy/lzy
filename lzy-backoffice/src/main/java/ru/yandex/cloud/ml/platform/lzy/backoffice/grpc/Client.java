package ru.yandex.cloud.ml.platform.lzy.backoffice.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.helpers.SubstituteLogger;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;

import java.util.logging.LogManager;


@Singleton
public class Client {
    private final Channel channel;

    @Inject
    Client(GrpcConfig config){

        channel = ManagedChannelBuilder.forAddress(config.getHost(), config.getPort()).build();
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
