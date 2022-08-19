package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.graph.Env;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.model.TaskDesc;
import ai.lzy.scheduler.servant.ServantApi;
import ai.lzy.scheduler.servant.ServantConnection;
import ai.lzy.v1.worker.Worker;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

public class ServantConnectionImpl implements ServantConnection {
    private final ManagedChannel channel;
    private final WorkerApiGrpc.WorkerApiBlockingStub servantBlockingStub;

    public ServantConnectionImpl(HostAndPort servantUrl) {
        this.channel = ChannelBuilder.forAddress(servantUrl.getHost(), servantUrl.getPort())
            .usePlaintext()
            .enableRetry(WorkerApiGrpc.SERVICE_NAME)
            .build();
        this.servantBlockingStub = WorkerApiGrpc.newBlockingStub(channel);
    }

    @Override
    public ServantApi api() {
        return new ServantApi() {
            @Override
            public void configure(Env env) throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.configure(Worker.ConfigureRequest.newBuilder()
                    .setEnv(GrpcConverter.to(env))
                    .build());
            }

            @Override
            public void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException {

                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.execute(Worker.ExecuteRequest.newBuilder()
                    .setTaskDesc(task.toProto())
                    .build());
            }

            @Override
            public void stop() throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.stop(Worker.StopRequest.newBuilder().build());
            }
        };
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
