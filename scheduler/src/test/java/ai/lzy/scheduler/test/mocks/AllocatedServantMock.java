package ai.lzy.scheduler.test.mocks;

import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.*;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AllocatedServantMock {
    private final Server server;
    private final Runnable onStop;
    private final Runnable onSignal;
    private final Runnable onEnv;
    private final Runnable onExec;

    public AllocatedServantMock(int port, Runnable onStop, Runnable onSignal,
                                Runnable onEnv, Runnable onExec) throws IOException {
        this.onStop = onStop;
        this.onSignal = onSignal;
        this.onEnv = onEnv;
        this.onExec = onExec;
        ServantImpl impl = new ServantImpl();
        server = NettyServerBuilder.forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(impl).build();
        server.start();
    }

    public void close() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

    private class ServantImpl extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void env(Operations.EnvSpec request, StreamObserver<Servant.EnvResult> responseObserver) {
            onEnv.run();
            responseObserver.onNext(Servant.EnvResult.newBuilder().setRc(0).setDescription("OK").build());
            responseObserver.onCompleted();
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            onStop.run();
            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            onExec.run();
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void signal(Tasks.TaskSignal request, StreamObserver<IAM.Empty> responseObserver) {
            onSignal.run();
            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    public static class ServantBuilder {
        private final int port;
        private Runnable onStop = () -> {};
        private Runnable onSignal = () -> {};
        private Runnable onEnv = () -> {};
        private Runnable onExec = () -> {};

        public ServantBuilder(int port) {
            this.port = port;
        }

        public ServantBuilder setOnStop(Runnable onStop) {
            this.onStop = onStop;
            return this;
        }

        public ServantBuilder setOnSignal(Runnable onSignal) {
            this.onSignal = onSignal;
            return this;
        }

        public ServantBuilder setOnEnv(Runnable onEnv) {
            this.onEnv = onEnv;
            return this;
        }

        public ServantBuilder setOnExec(Runnable onExec) {
            this.onExec = onExec;
            return this;
        }

        public AllocatedServantMock build() throws IOException {
            return new AllocatedServantMock(port, onStop, onSignal, onEnv, onExec);
        }
    }

}
