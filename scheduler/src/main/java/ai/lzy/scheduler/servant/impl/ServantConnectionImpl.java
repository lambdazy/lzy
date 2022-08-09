package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.Slot;
import ai.lzy.model.graph.Env;
import ai.lzy.util.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.Tasks;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantApi;
import ai.lzy.scheduler.servant.ServantConnection;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import java.util.stream.Stream;

public class ServantConnectionImpl implements ServantConnection {
    private final ManagedChannel channel;
    private final LzyServantGrpc.LzyServantBlockingStub servantBlockingStub;
    private final Servant servant;

    public ServantConnectionImpl(HostAndPort servantUrl, Servant servant) {
        this.servant = servant;
        this.channel = ChannelBuilder.forAddress(servantUrl.getHost(), servantUrl.getPort())
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        this.servantBlockingStub = LzyServantGrpc.newBlockingStub(channel);
    }

    @Override
    public ServantApi api() {
        return new ServantApi() {
            @Override
            public void configure(Env env) throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.env(GrpcConverter.to(env));
            }

            @Override
            public void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException {
                Tasks.TaskSpec.Builder builder = Tasks.TaskSpec.newBuilder()
                    .setZygote(GrpcConverter.to(task.zygote()))
                    .setTid(taskId);
                task.zygote().slots().forEach(slot -> {
                    if (Stream.of(Slot.STDOUT, Slot.STDERR)
                        .map(Slot::name)
                        .noneMatch(s -> s.equals(slot.name()))) {
                        builder.addAssignmentsBuilder()
                            .setSlot(GrpcConverter.to(slot))
                            .setBinding(task.slotsToChannelsAssignments().get(slot.name()))
                            .build();
                    }
                });
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.execute(builder.build());
            }

            @Override
            public void gracefulStop() throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.stop(IAM.Empty.newBuilder().build());
            }

            @Override
            public void signal(int signalNumber) throws StatusRuntimeException {
                //noinspection ResultOfMethodCallIgnored
                servantBlockingStub.signal(Tasks.TaskSignal.newBuilder()
                    .setSigValue(signalNumber)
                    .build());
            }
        };
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
