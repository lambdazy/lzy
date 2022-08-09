package ai.lzy.server.mocks;

import ai.lzy.util.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Servant;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
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
            } else {
                responseObserver.onNext(Servant.EnvResult.newBuilder().setRc(-1).setDescription("NOT OK").build());
            }
            responseObserver.onCompleted();
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
