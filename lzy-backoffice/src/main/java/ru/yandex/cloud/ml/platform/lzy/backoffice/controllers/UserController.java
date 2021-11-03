package ru.yandex.cloud.ml.platform.lzy.backoffice.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import ru.yandex.cloud.ml.platform.lzy.backoffice.grpc.Client;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.*;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

import javax.validation.Valid;

@Controller("users")
public class UserController {
    @Inject
    Client client;

    @Post("add_token")
    public HttpResponse<?> addToken(@Valid @Body AddTokenRequest request){
        client.addToken(request);
        return HttpResponse.ok();
    }

    @Post("create")
    public HttpResponse<?> create(@Valid @Body CreateUserRequest request){
        client.createUser(request);
        return HttpResponse.ok();
    }

    @Post("delete")
    public HttpResponse<?> delete(@Valid @Body DeleteUserRequest request){
        client.deleteUser(request);
        return HttpResponse.ok();
    }
    @Post("list")
    public HttpResponse<ListUsersResponse> list(@Valid @Body ListUsersRequest request){
        return HttpResponse.ok(ListUsersResponse.fromModel(client.listUsers(request)));
    }
}
