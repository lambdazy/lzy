package ai.lzy.backoffice.controllers;

import ai.lzy.backoffice.grpc.Client;
import ai.lzy.backoffice.models.users.CreateUserRequest;
import ai.lzy.backoffice.models.users.DeleteUserRequest;
import ai.lzy.backoffice.models.users.ListUsersRequest;
import ai.lzy.backoffice.models.users.ListUsersResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;

import javax.validation.Valid;

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
