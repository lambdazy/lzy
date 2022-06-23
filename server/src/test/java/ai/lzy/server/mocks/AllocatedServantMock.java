package ai.lzy.server.mocks;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import ai.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AllocatedServantMock {
    private final ServantImpl impl;
    private final boolean failEnv;
    private final Consumer<AllocatedServantMock> onStop;
    private final Server server;

    public AllocatedServantMock(
            boolean failEnv,
            Consumer<AllocatedServantMock> onStop,
            int port
        ) throws IOException {
        this.impl = new ServantImpl();
        server = NettyServerBuilder.forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(impl).build();
        server.start();
        this.failEnv = failEnv;
        this.onStop = onStop;
    }

    public void progress(Servant.ServantProgress progress) {
        impl.responseObserver.onNext(progress);
    }

    public void complete(StatusRuntimeException exception) {
        if (exception == null) {
            impl.responseObserver.onCompleted();
        } else {
            impl.responseObserver.onError(exception);
        }
    }

    public void close() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

    private class ServantImpl extends LzyServantGrpc.LzyServantImplBase {
        private StreamObserver<Servant.ServantProgress> responseObserver;

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            this.responseObserver = responseObserver;
            Servant.ServantProgress start = Servant.ServantProgress.newBuilder()
                .setStart(Servant.Started.newBuilder().build())
                .build();
            responseObserver.onNext(start);
        }

        @Override
        public void env(Operations.EnvSpec request, StreamObserver<Servant.EnvResult> responseObserver) {
            if (!failEnv) {
                responseObserver.onNext(Servant.EnvResult.newBuilder().setRc(0).setDescription("OK").build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad exception").asException());
            }
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            try {
                onStop.accept(AllocatedServantMock.this);
                responseObserver.onNext(IAM.Empty.newBuilder().build());
                responseObserver.onCompleted();
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            }
        }
    }
}
