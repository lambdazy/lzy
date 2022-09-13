package ai.lzy.site.controllers;

import ai.lzy.site.models.users.CreateUserRequest;
import ai.lzy.site.models.users.DeleteUserRequest;
import ai.lzy.site.models.users.ListUsersRequest;
import ai.lzy.site.models.users.ListUsersResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import javax.validation.Valid;
import ai.lzy.site.grpc.Client;

@ExecuteOn(TaskExecutors.IO)
@Controller("users")
public class UserController {

    @Inject
    Client client;

    @Post("create")
    public HttpResponse<?> create(@Valid @Body CreateUserRequest request) {
        client.createUser(request);
        return HttpResponse.ok();
    }

    @Post("delete")
    public HttpResponse<?> delete(@Valid @Body DeleteUserRequest request) {
        client.deleteUser(request);
        return HttpResponse.ok();
    }

    @Post("list")
    public HttpResponse<ListUsersResponse> list(@Valid @Body ListUsersRequest request) {
        return HttpResponse.ok(ListUsersResponse.fromModel(client.listUsers(request)));
    }
}
