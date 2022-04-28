package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.UUID;

public class ServerConnection {
    private final LzyServerGrpc.LzyServerBlockingStub lzyServer;
    private final StreamObserver<Servant.ServantProgress> progress;

    public ServerConnection(
        LzyServerGrpc.LzyServerBlockingStub lzyServer,
        StreamObserver<Servant.ServantProgress> progress
    ) {
        this.lzyServer = lzyServer;
        this.progress = progress;
    }

    void register(IAM.UserCredentials auth, URI servantUri, UUID servantId) {
        final IAM.UserCredentials userCredentials = IAM.UserCredentials.newBuilder()
                .setUserId(auth.getUserId())
                .setToken(auth.getToken())
                .build();

        //noinspection ResultOfMethodCallIgnored
        lzyServer.registerServant(Lzy.AttachServant.newBuilder()
                .setAuth(IAM.Auth.newBuilder()
                        .setUser(userCredentials)
                        .build())
                .setServantURI(servantUri.toString())
                .setServantId(servantId.toString())
                .build());
    }

    void attach(Servant.SlotAttach attach) {
        progress.onNext(Servant.ServantProgress.newBuilder()
            .setAttach(Servant.SlotAttach.newBuilder()
                .setSlot(attach.getSlot())
                .setUri(convertToKharonServantUri(attach.getUri()))
                .setChannel(attach.getChannel())
                .build())
            .build());
    }

    void detach(Servant.SlotDetach detach) {
        progress.onNext(Servant.ServantProgress.newBuilder()
            .setDetach(Servant.SlotDetach.newBuilder()
                .setSlot(detach.getSlot())
                .setUri(convertToKharonServantUri(detach.getUri()))
                .build())
            .build());
    }

    void terminate(Throwable th) {
        progress.onError(th);
    }

    void complete() {
        progress.onCompleted();
    }
}
