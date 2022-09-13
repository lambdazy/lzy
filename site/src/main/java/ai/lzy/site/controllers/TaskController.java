package ai.lzy.site.controllers;

import ai.lzy.site.grpc.Client;
import ai.lzy.site.models.tasks.GetTasksRequest;
import ai.lzy.site.models.tasks.GetTasksResponse;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.SchedulerApi;
import ai.lzy.v1.SchedulerGrpc;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import javax.validation.Valid;

@ExecuteOn(TaskExecutors.IO)
@Controller("tasks")
public class TaskController {

    @Inject
    SchedulerGrpc.SchedulerBlockingStub scheduler;

    @Post("get")
    public HttpResponse<GetTasksResponse> get(@Valid @Body GetTasksRequest request) {
        scheduler
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION,
                () -> request.getCredentials()
            ))
            .list(SchedulerApi.TaskListRequest.newBuilder()
            .build());
        return HttpResponse.ok(
            GetTasksResponse.fromModel()
        );
    }
}
