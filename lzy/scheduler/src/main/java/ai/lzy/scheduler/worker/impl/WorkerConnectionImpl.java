package ai.lzy.scheduler.worker.impl;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.graph.Env;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.scheduler.worker.WorkerApi;
import ai.lzy.scheduler.worker.WorkerConnection;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

public class WorkerConnectionImpl implements WorkerConnection {
    private final ManagedChannel channel;
    private final WorkerApiGrpc.WorkerApiBlockingStub workerBlockingStub;

    public WorkerConnectionImpl(HostAndPort workerUrl) {
        this.channel = ChannelBuilder.forAddress(workerUrl.getHost(), workerUrl.getPort())
            .usePlaintext()
            .enableRetry(WorkerApiGrpc.SERVICE_NAME)
            .build();
        this.workerBlockingStub = WorkerApiGrpc.newBlockingStub(channel);
    }

    @Override
    public WorkerApi api() {
        return new WorkerApi() {
            @Override
            public void configure(Env env) throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                workerBlockingStub.configure(LWS.ConfigureRequest.newBuilder()
                    .setEnv(ProtoConverter.toProto(env))
                    .build());
            }

            @Override
            public void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException {

                //noinspection ResultOfMethodCallIgnored
                workerBlockingStub.execute(LWS.ExecuteRequest.newBuilder()
                    .setTaskDesc(task.toProto())
                    .setTaskId(taskId)
                    .build());
            }

            @Override
            public void stop() throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                workerBlockingStub.stop(LWS.StopRequest.newBuilder().build());
            }
        };
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
