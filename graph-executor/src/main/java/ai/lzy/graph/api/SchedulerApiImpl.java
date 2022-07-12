package ai.lzy.graph.api;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v2.SchedulerApi.*;
import ai.lzy.priv.v2.SchedulerGrpc;
import ai.lzy.priv.v2.SchedulerGrpc.SchedulerBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;

@Singleton
public class SchedulerApiImpl implements SchedulerApi {
    private final SchedulerBlockingStub stub;
    private final ManagedChannel channel;

    @Inject
    public SchedulerApiImpl(ServiceConfig config) {
        channel = ChannelBuilder.forAddress(config.scheduler().host(), config.scheduler().port())
            .usePlaintext()
            .enableRetry(SchedulerGrpc.SERVICE_NAME)
            .build();
        stub = SchedulerGrpc.newBlockingStub(channel);
    }

    @Override
    public TaskStatus execute(String workflowId, TaskDescription task) {
        var res = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId(workflowId)
            .setTask(TaskDesc.newBuilder()
                .setZygote(GrpcConverter.to(task.zygote()))
                .addAllSlotAssignments(task.slotsToChannelsAssignments().entrySet().stream()
                    .map(entry -> SlotToChannelAssignment.newBuilder()
                        .setSlotName(entry.getKey())
                        .setChannelId(entry.getValue())
                        .build()
                    ).toList())
                .build())
            .build());
        return res.getStatus();
    }

    @Nullable
    @Override
    public TaskStatus status(String workflowId, String taskId) {
        try {
            var res = stub.status(TaskStatusRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId)
                .build());
            return res.getStatus();
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.NOT_FOUND) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public TaskStatus kill(String workflowId, String taskId) {
        var res = stub.stop(TaskStopRequest.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskId(taskId)
            .setIssue("Killing by GraphExecutor")
            .build());
        return res.getStatus();
    }
}
