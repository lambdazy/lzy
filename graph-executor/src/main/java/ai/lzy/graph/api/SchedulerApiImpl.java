package ai.lzy.graph.api;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.model.TaskDesc;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;
import ai.lzy.v1.scheduler.SchedulerApi.TaskScheduleRequest;
import ai.lzy.v1.scheduler.SchedulerApi.TaskStatusRequest;
import ai.lzy.v1.scheduler.SchedulerApi.TaskStopRequest;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import ai.lzy.v1.scheduler.SchedulerGrpc.SchedulerBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import javax.annotation.PreDestroy;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class SchedulerApiImpl implements SchedulerApi {
    private final SchedulerBlockingStub stub;
    private final ManagedChannel channel;

    @Inject
    public SchedulerApiImpl(ServiceConfig config, RenewableJwt iamToken) {
        channel = newGrpcChannel(config.getScheduler().getHost(), config.getScheduler().getPort(),
            SchedulerGrpc.SERVICE_NAME);

        stub = newBlockingClient(SchedulerGrpc.newBlockingStub(channel), "LzyScheduler", () -> iamToken.get().token());
    }

    @Override
    public TaskStatus execute(String workflowName, String workflowId, TaskDescription task) {
        var res = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setWorkflowId(workflowId)
            .setTask(new TaskDesc(task.operation(), task.slotsToChannelsAssignments()).toProto())
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

    @PreDestroy
    public void shutdown() {
        GrpcChannels.awaitTermination(channel, Duration.ofSeconds(10), getClass());
    }
}
