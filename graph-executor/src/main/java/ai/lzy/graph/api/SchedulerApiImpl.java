package ai.lzy.graph.api;

import ai.lzy.graph.config.AuthConfig;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.model.TaskDesc;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.SchedulerApi.*;
import ai.lzy.v1.SchedulerGrpc;
import ai.lzy.v1.SchedulerGrpc.SchedulerBlockingStub;
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
    public SchedulerApiImpl(ServiceConfig config, AuthConfig authConfig) {
        channel = ChannelBuilder.forAddress(config.scheduler().host(), config.scheduler().port())
            .usePlaintext()
            .enableRetry(SchedulerGrpc.SERVICE_NAME)
            .build();

        final var credentials = JwtUtils.credentials(authConfig.serviceUid(),
            authConfig.privateKey());
        stub = SchedulerGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
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
}
