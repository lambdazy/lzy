package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.basic.TaskDesc;
import ai.lzy.model.graph.Env;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.scheduler.servant.ServantApi;
import ai.lzy.scheduler.servant.ServantConnection;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.worker.LWS;
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
                servantBlockingStub.configure(LWS.ConfigureRequest.newBuilder()
                    .setEnv(ProtoConverter.toProto(env))
                    .build());
            }

            @Override
            public void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException {

                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.execute(LWS.ExecuteRequest.newBuilder()
                    .setTaskDesc(task.toProto())
                    .setTaskId(taskId)
                    .build());
            }

            @Override
            public void stop() throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.stop(LWS.StopRequest.newBuilder().build());
            }
        };
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
