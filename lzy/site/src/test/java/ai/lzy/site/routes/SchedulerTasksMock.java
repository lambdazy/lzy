package ai.lzy.site.routes;

import ai.lzy.v1.scheduler.Scheduler;
import ai.lzy.v1.scheduler.SchedulerApi.TaskListRequest;
import ai.lzy.v1.scheduler.SchedulerApi.TaskListResponse;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
@Primary
@Requires(env = "site-test")
public final class SchedulerTasksMock extends SchedulerGrpc.SchedulerImplBase {
    @Override
    public void list(TaskListRequest request, StreamObserver<TaskListResponse> responseObserver) {
        final String workflowId = request.getWorkflowId();
        final var responseBuilder = TaskListResponse.newBuilder();
        getTestTasks(workflowId)
            .forEach(taskStatus -> {
                final Scheduler.TaskStatus.Builder taskBuilder = Scheduler.TaskStatus.newBuilder()
                    .setWorkflowId(taskStatus.workflowId())
                    .setTaskId(taskStatus.taskId())
                    .setOperationName(taskStatus.operationName());
                switch (taskStatus.status()) {
                    case "SUCCESS" -> taskBuilder.setSuccess(
                        Scheduler.TaskStatus.Success.newBuilder().setRc(0).build());
                    case "EXECUTING" -> taskBuilder.setExecuting(
                        Scheduler.TaskStatus.Executing.getDefaultInstance());
                    case "ERROR" -> taskBuilder.setError(Scheduler.TaskStatus.Error.newBuilder()
                        .setDescription(taskStatus.description())
                        .buildPartial());
                }
                responseBuilder.addStatus(taskBuilder.build());
            });

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    public static List<Tasks.TaskStatus> getTestTasks(String workflowId) {
        return List.of(
            new Tasks.TaskStatus(workflowId, "foo", "task1", "SUCCESS", "Return code: 0"),
            new Tasks.TaskStatus(workflowId, "bar", "task2", "EXECUTING", "-"),
            new Tasks.TaskStatus(workflowId, "baz", "task3", "ERROR", "some error occurred")
        );
    }
}