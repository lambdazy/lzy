package ai.lzy.scheduler.servant.impl;

import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantApi;
import ai.lzy.scheduler.servant.ServantConnection;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.net.URL;
import java.util.Arrays;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.ServantApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.ServantApiV2Grpc;
import yandex.cloud.priv.datasphere.v2.lzy.ServantV2;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

public class ServantConnectionImpl implements ServantConnection {
    private final ManagedChannel channel;
    private final ServantApiV2Grpc.ServantApiV2BlockingStub servantBlockingStub;

    public ServantConnectionImpl(URL servantUrl) {
        this.channel = ChannelBuilder.forAddress(servantUrl.getHost(), servantUrl.getPort())
            .usePlaintext()
            .enableRetry(ServantApiGrpc.SERVICE_NAME)
            .build();
        this.servantBlockingStub = ServantApiV2Grpc.newBlockingStub(channel);
    }

    @Override
    public ServantApi api() {
        return new ServantApi() {
            @Override
            public void configure(Env env) throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.configure(ServantV2.ConfigureRequest.newBuilder()
                    .setSpec(GrpcConverter.to(env))
                    .build());
            }

            @Override
            public void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException {
                Tasks.TaskSpec.Builder builder = Tasks.TaskSpec.newBuilder()
                    .setZygote(GrpcConverter.to(task.zygote()))
                    .setTid(taskId);
                Arrays.stream(task.zygote().input()).forEach(slot -> {
                    if (Stream.of(Slot.STDIN, Slot.STDOUT, Slot.STDERR)
                        .map(Slot::name)
                        .noneMatch(s -> s.equals(slot.name()))) {
                        builder.addAssignmentsBuilder()
                            .setSlot(GrpcConverter.to(slot))
                            .setBinding(task.slotsToChannelsAssignments().get(slot.name()))
                            .build();
                    }
                });
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.execute(ServantV2.ExecuteRequest.newBuilder()
                    .setSpec(builder.build())
                    .build());
            }

            @Override
            public void gracefulStop() throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.stop(ServantV2.StopRequest.newBuilder().build());
            }

            @Override
            public void signal(int signalNumber) throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.signal(ServantV2.SignalRequest.newBuilder()
                    .setSignal(signalNumber)
                    .build());
            }
        };
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
