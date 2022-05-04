package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

public class ServerController {
    private static final Logger LOG = LogManager.getLogger(ServerController.class);

    private final LzyServerGrpc.LzyServerBlockingStub lzyServer;
    private StreamObserver<Servant.ServantProgress> progress;
    private final UriResolver uriResolver;
    private final UUID sessionId;

    private enum State {
        UNBOUND,
        REGISTERED,
        CONNECTED,
        ERRORED,
        COMPLETED
    }

    private State state;

    public ServerController(
            LzyServerGrpc.LzyServerBlockingStub lzyServer,
            UriResolver uriResolver, UUID sessionId) {
        this.lzyServer = lzyServer;
        this.uriResolver = uriResolver;
        this.sessionId = sessionId;
        this.state = State.UNBOUND;
    }

    public void register(IAM.UserCredentials auth) {
        final IAM.UserCredentials userCredentials = IAM.UserCredentials.newBuilder()
                .setUserId(auth.getUserId())
                .setToken(auth.getToken())
                .build();

        //noinspection ResultOfMethodCallIgnored
        lzyServer.registerServant(Lzy.AttachServant.newBuilder()
            .setAuth(
                IAM.Auth.newBuilder()
                .setUser(userCredentials)
                .build())
            .setServantURI(servantUri.toString())
            .setServantId(sessionId.toString())
            .build());
        updateState(State.REGISTERED);
    }

    public void setProgress(StreamObserver<Servant.ServantProgress> progress) {
        this.progress = progress;
        updateState(State.CONNECTED);
    }

    public void attach(Servant.SlotAttach attach) throws URISyntaxException {
        waitForConnection();
        progress.onNext(Servant.ServantProgress.newBuilder()
            .setAttach(Servant.SlotAttach.newBuilder()
                .setSlot(attach.getSlot())
                .setUri(uriResolver.convertSlotUri(URI.create(attach.getUri()), sessionId).toString())
                .setChannel(attach.getChannel())
                .build())
            .build());
    }

    public void detach(Servant.SlotDetach detach) throws URISyntaxException {
        waitForConnection();
        progress.onNext(Servant.ServantProgress.newBuilder()
            .setDetach(Servant.SlotDetach.newBuilder()
                .setSlot(detach.getSlot())
                .setUri(uriResolver.convertSlotUri(URI.create(detach.getUri()), sessionId).toString())
                .build())
            .build());
    }

    public void terminate(Throwable th) {
        progress.onError(th);
        updateState(State.ERRORED);
    }

    public void complete() {
        progress.onCompleted();
        updateState(State.COMPLETED);
    }

    private synchronized void updateState(State state) {
        this.state = state;
        notifyAll();
    }

    private synchronized void waitForConnection() {
        while (State.CONNECTED != state && state != State.ERRORED && state != State.COMPLETED) {
            try {
                wait();
            } catch (InterruptedException ignore) {
                // Ignored exception
            }
        }

        if (state == State.ERRORED || state == State.COMPLETED) {
            throw new
        }
    }
}
