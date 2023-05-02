package ai.lzy.site.routes;

import ai.lzy.site.AuthUtils;
import ai.lzy.v1.scheduler.Scheduler;
import ai.lzy.v1.scheduler.SchedulerApi;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.CookieValue;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;
import jakarta.validation.Valid;

import java.util.List;

@Controller("/tasks")
public class Tasks {
    @Inject
    AuthUtils authUtils;

    @Inject
    SchedulerGrpc.SchedulerBlockingStub scheduler;

    @Post("get")
    public HttpResponse<GetTasksResponse> get(@CookieValue String userSubjectId, @CookieValue String sessionId,
                                              @Valid @Body GetTasksRequest tasksRequest)
    {
        authUtils.checkCookieAndGetSubject(userSubjectId, sessionId);
        final SchedulerApi.TaskListResponse taskListResponse = scheduler
            .list(SchedulerApi.TaskListRequest.newBuilder()
                .setWorkflowId(tasksRequest.workflowId())
                .build());
        return HttpResponse.ok(
            GetTasksResponse.fromProto(taskListResponse)
        );
    }

    @Introspected
    public record GetTasksRequest(String workflowId) {
    }

    @Introspected
    public record GetTasksResponse(
        List<TaskStatus> taskStatusList
    )
    {
        public static GetTasksResponse fromProto(SchedulerApi.TaskListResponse taskListResponse) {
            return new GetTasksResponse(taskListResponse.getStatusList().stream()
                .map(TaskStatus::fromProto)
                .toList());
        }
    }

    public record TaskStatus(
        String workflowId,
        String taskId,
        String operationName,
        String status,
        String description
    )
    {
        public static TaskStatus fromProto(Scheduler.TaskStatus status) {
            return new TaskStatus(
                status.getWorkflowId(),
                status.getTaskId(),
                status.getOperationName(),
                status.getStatusCase().name(),
                switch (status.getStatusCase()) {
                    case SUCCESS -> "Return code: " + status.getSuccess().getRc();
                    case ERROR -> status.getError().getDescription();
                    default -> "-";
                }
            );
        }
    }
}
