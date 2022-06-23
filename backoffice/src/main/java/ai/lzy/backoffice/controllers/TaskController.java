package ai.lzy.backoffice.controllers;

import ai.lzy.backoffice.grpc.Client;
import ai.lzy.backoffice.models.tasks.GetTasksRequest;
import ai.lzy.backoffice.models.tasks.GetTasksResponse;
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
    Client client;

    @Post("get")
    public HttpResponse<GetTasksResponse> get(@Valid @Body GetTasksRequest request) {
        return HttpResponse.ok(
            GetTasksResponse.fromModel(client.getTasks(request))
        );
    }
}
