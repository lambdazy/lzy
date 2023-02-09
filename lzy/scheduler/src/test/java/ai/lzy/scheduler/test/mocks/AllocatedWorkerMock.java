package ai.lzy.scheduler.test.mocks;

import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS.*;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class AllocatedWorkerMock {
    private final Server server;
    private final Function<ExecuteRequest, Boolean> onExec;
    private final LocalOperationService opService;

    public AllocatedWorkerMock(int port, Function<ExecuteRequest, Boolean> onExec) throws IOException {
        this.onExec = onExec;
        this.opService = new LocalOperationService("worker");
        WorkerImpl impl = new WorkerImpl();
        server = NettyServerBuilder.forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(impl)
            .addService(opService)
            .build();
        server.start();
    }

    public void close() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

    private class WorkerImpl extends WorkerApiGrpc.WorkerApiImplBase {

        @Override
        public void execute(ExecuteRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
            var op = Operation.create("test", "", null, null);
            opService.registerOperation(op);

            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();

            var success = onExec.apply(request);
            if (success) {
                opService.updateResponse(op.id(), ExecuteResponse.newBuilder()
                    .setRc(0)
                    .setDescription("Ok")
                    .build());
            } else {
                opService.updateResponse(op.id(), ExecuteResponse.newBuilder()
                    .setRc(1)
                    .setDescription("Fail")
                    .build());
            }
        }
    }
}
