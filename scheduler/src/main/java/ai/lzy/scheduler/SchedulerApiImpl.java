package ai.lzy.scheduler;

import ai.lzy.model.TaskDesc;
import ai.lzy.scheduler.servant.Scheduler;
import ai.lzy.scheduler.task.Task;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;
import ai.lzy.v1.scheduler.SchedulerApi.*;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SchedulerApiImpl extends SchedulerGrpc.SchedulerImplBase {
    private final Scheduler scheduler;

    @Inject
    public SchedulerApiImpl(Scheduler scheduler) {
        this.scheduler = scheduler;
        scheduler.start();
    }

    @Override
    public void schedule(TaskScheduleRequest request, StreamObserver<TaskScheduleResponse> responseObserver) {
        final Task task;
        try {
            task = scheduler.execute(request.getWorkflowId(), request.getWorkflowName(),
                    TaskDesc.fromProto(request.getTask()));
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(TaskScheduleResponse.newBuilder()
                .setStatus(buildTaskStatus(task))
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void status(TaskStatusRequest request, StreamObserver<TaskStatusResponse> responseObserver) {
        final Task task;
        try {
            task = scheduler.status(request.getWorkflowId(), request.getTaskId());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(TaskStatusResponse.newBuilder()
                .setStatus(buildTaskStatus(task))
                .buildPartial());
        responseObserver.onCompleted();
    }

    @Override
    public void list(TaskListRequest request, StreamObserver<TaskListResponse> responseObserver) {
        final List<Task> tasks;
        try {
            tasks = scheduler.list(request.getWorkflowId());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        List<TaskStatus> statuses = tasks.stream()
                .map(SchedulerApiImpl::buildTaskStatus)
                .toList();
        responseObserver.onNext(TaskListResponse.newBuilder().addAllStatus(statuses).build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(TaskStopRequest request, StreamObserver<TaskStopResponse> responseObserver) {
        final Task task;
        try {
            task = scheduler.stopTask(request.getWorkflowId(), request.getTaskId(), request.getIssue());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(TaskStopResponse.newBuilder().setStatus(buildTaskStatus(task)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void killAll(KillAllRequest request, StreamObserver<KillAllResponse> responseObserver) {
        try {
            scheduler.killAll(request.getWorkflowName(), request.getIssue());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(KillAllResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private static TaskStatus buildTaskStatus(Task task) {
        var builder = TaskStatus.newBuilder()
                .setTaskId(task.taskId())
                .setWorkflowId(task.workflowId())
                .setZygoteName(task.description().operation().name());

        Integer rc = task.rc();
        int rcInt = rc == null ? 0 : rc;
        var b = switch (task.status()) {
            case QUEUE -> builder.setQueue(TaskStatus.Queue.newBuilder().build());
            case SCHEDULED, EXECUTING -> builder.setExecuting(TaskStatus.Executing.newBuilder().build());
            case ERROR -> builder.setError(TaskStatus.Error.newBuilder()
                    .setDescription(task.errorDescription())
                    .setRc(rcInt)
                    .build());
            case SUCCESS -> builder.setSuccess(TaskStatus.Success.newBuilder().setRc(rcInt).build());
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
