package ru.yandex.cloud.ml.platform.lzy.backoffice.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import ru.yandex.cloud.ml.platform.lzy.backoffice.grpc.Client;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.AddTokenRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.User;
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
}
