package ai.lzy.scheduler.test.mocks;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.worker.LWS.*;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AllocatedWorkerMock {
    private final Server server;
    private final Runnable onStop;
    private final Runnable onSignal;
    private final Runnable onEnv;
    private final Runnable onExec;

    public AllocatedWorkerMock(int port, Runnable onStop, Runnable onSignal,
                               Runnable onEnv, Runnable onExec) throws IOException
    {
        this.onStop = onStop;
        this.onSignal = onSignal;
        this.onEnv = onEnv;
        this.onExec = onExec;
        WorkerImpl impl = new WorkerImpl();
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

    private class WorkerImpl extends WorkerApiGrpc.WorkerApiImplBase {

        @Override
        public void configure(ConfigureRequest request, StreamObserver<ConfigureResponse> responseObserver) {
            onEnv.run();
            responseObserver.onNext(ConfigureResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void stop(StopRequest request, StreamObserver<StopResponse> responseObserver) {
            onStop.run();
            responseObserver.onNext(StopResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void execute(ExecuteRequest request, StreamObserver<ExecuteResponse> responseObserver) {
            onExec.run();
            responseObserver.onNext(ExecuteResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    public static class WorkerBuilder {
        private final int port;
        private Runnable onStop = () -> {};
        private Runnable onSignal = () -> {};
        private Runnable onEnv = () -> {};
        private Runnable onExec = () -> {};

        public WorkerBuilder(int port) {
            this.port = port;
        }

        public WorkerBuilder setOnStop(Runnable onStop) {
            this.onStop = onStop;
            return this;
        }

        public WorkerBuilder setOnSignal(Runnable onSignal) {
            this.onSignal = onSignal;
            return this;
        }

        public WorkerBuilder setOnEnv(Runnable onEnv) {
            this.onEnv = onEnv;
            return this;
        }

        public WorkerBuilder setOnExec(Runnable onExec) {
            this.onExec = onExec;
            return this;
        }

        public AllocatedWorkerMock build() throws IOException {
            return new AllocatedWorkerMock(port, onStop, onSignal, onEnv, onExec);
        }
    }

}
