package ai.lzy.scheduler;

import ai.lzy.priv.v2.SchedulerApi;
import ai.lzy.priv.v2.SchedulerGrpc;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.Scheduler;
import ai.lzy.scheduler.task.Task;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class SchedulerApiImpl extends SchedulerGrpc.SchedulerImplBase {
    private final Scheduler scheduler;

    @Inject
    public SchedulerApiImpl(Scheduler scheduler) {
        this.scheduler = scheduler;
        scheduler.start();
    }

    @Override
    public void schedule(SchedulerApi.TaskScheduleRequest request, StreamObserver<SchedulerApi.TaskScheduleResponse> responseObserver) {
        final Task task;
        try {
            task = scheduler.execute(request.getWorkflowId(), request.getWorkflowName(),
                    TaskDesc.fromGrpc(request.getTask()));
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(SchedulerApi.TaskScheduleResponse.newBuilder()
                .setStatus(buildTaskStatus(task))
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void status(SchedulerApi.TaskStatusRequest request, StreamObserver<SchedulerApi.TaskStatusResponse> responseObserver) {
        final Task task;
        try {
            task = scheduler.status(request.getWorkflowId(), request.getTaskId());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(SchedulerApi.TaskStatusResponse.newBuilder()
                .setStatus(buildTaskStatus(task))
                .buildPartial());
        responseObserver.onCompleted();
    }

    @Override
    public void list(SchedulerApi.TaskListRequest request, StreamObserver<SchedulerApi.TaskListResponse> responseObserver) {
        final List<Task> tasks;
        try {
            tasks = scheduler.list(request.getWorkflowId());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        List<SchedulerApi.TaskStatus> statuses = tasks.stream()
                .map(SchedulerApiImpl::buildTaskStatus)
                .toList();
        responseObserver.onNext(SchedulerApi.TaskListResponse.newBuilder().addAllStatus(statuses).build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(SchedulerApi.TaskStopRequest request, StreamObserver<SchedulerApi.TaskStopResponse> responseObserver) {
        final Task task;
        try {
            task = scheduler.stopTask(request.getWorkflowId(), request.getTaskId(), request.getIssue());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(SchedulerApi.TaskStopResponse.newBuilder().setStatus(buildTaskStatus(task)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void killAll(SchedulerApi.KillAllRequest request, StreamObserver<SchedulerApi.KillAllResponse> responseObserver) {
        try {
            scheduler.killAll(request.getWorkflowName(), request.getIssue());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(SchedulerApi.KillAllResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private static SchedulerApi.TaskStatus buildTaskStatus(Task task) {
        var builder = SchedulerApi.TaskStatus.newBuilder()
                .setTaskId(task.taskId())
                .setWorkflowId(task.workflowId())
                .setZygoteName(task.description().zygote().name());

        Integer rc = task.rc();
        int rcInt = rc == null ? 0 : rc;
        var b = switch (task.status()) {
            case QUEUE -> builder.setQueue(SchedulerApi.TaskStatus.Queue.newBuilder().build());
            case SCHEDULED, EXECUTING -> builder.setExecuting(SchedulerApi.TaskStatus.Executing.newBuilder().build());
            case ERROR -> builder.setError(SchedulerApi.TaskStatus.Error.newBuilder()
                    .setDescription(task.errorDescription())
                    .setRc(rcInt)
                    .build());
            case SUCCESS -> builder.setSuccess(SchedulerApi.TaskStatus.Success.newBuilder()
                    .setRc(rcInt)
                    .build());
        };
        return b.build();
    }

    public void close() {
        scheduler.terminate();
    }

    public void awaitTermination() throws InterruptedException {
        scheduler.awaitTermination();
    }
}
