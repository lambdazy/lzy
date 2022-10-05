package ai.lzy.site.routes;

import ai.lzy.site.AuthUtils;
import ai.lzy.site.Cookie;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.SchedulerApi;
import ai.lzy.v1.SchedulerGrpc;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.CookieValue;
import io.micronaut.http.annotation.Get;
import io.micronaut.validation.Validated;
import jakarta.inject.Inject;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

@Validated
@Controller("/tasks")
public class Tasks {
    @Inject
    AuthUtils authUtils;

    @Inject
    SchedulerGrpc.SchedulerBlockingStub scheduler;

    @Get("get")
    public HttpResponse<GetTasksResponse> get(@Valid @Body GetTasksRequest tasksRequest) {
        authUtils.checkCookieAndGetSubject(tasksRequest.cookie);
        final SchedulerApi.TaskListResponse taskListResponse = scheduler
            .list(SchedulerApi.TaskListRequest.newBuilder()
                .setWorkflowId(tasksRequest.workflowId())
                .build());
        return HttpResponse.ok(
            GetTasksResponse.fromProto(taskListResponse)
        );
    }

    @Introspected
    public record GetTasksRequest(
        Cookie cookie,
        String workflowId
    ) {}

    @Introspected
    public record GetTasksResponse(
        List<TaskStatus> taskStatusList
    ) {
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
    ) {
        public static TaskStatus fromProto(SchedulerApi.TaskStatus status) {
            return new TaskStatus(
                status.getWorkflowId(),
                status.getTaskId(),
                status.getZygoteName(),
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
