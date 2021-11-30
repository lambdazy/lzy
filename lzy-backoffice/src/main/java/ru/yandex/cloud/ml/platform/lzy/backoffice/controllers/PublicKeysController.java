package ru.yandex.cloud.ml.platform.lzy.backoffice.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import ru.yandex.cloud.ml.platform.lzy.backoffice.grpc.Client;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.keys.AddPublicKeyRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.keys.DeletePublicKeyRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.keys.ListKeysRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.keys.ListKeysResponse;

import javax.validation.Valid;

@ExecuteOn(TaskExecutors.IO)
@Controller("public_keys")
public class PublicKeysController {
    @Inject
    Client client;

    @Post("add")
    public HttpResponse<?> add(@Valid @Body AddPublicKeyRequest request){
        client.addToken(request);
        return HttpResponse.ok();
    }

    @Post("delete")
    public HttpResponse<?> delete(@Valid @Body DeletePublicKeyRequest request){
        client.deleteToken(request);
        return HttpResponse.ok();
    }

    @Post("list")
    public HttpResponse<ListKeysResponse> list(@Valid @Body ListKeysRequest request){
        return HttpResponse.ok(ListKeysResponse.fromModel(client.listTokens(request)));
    }


}
