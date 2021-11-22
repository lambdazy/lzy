package ru.yandex.cloud.ml.platform.lzy.backoffice.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import ru.yandex.cloud.ml.platform.lzy.backoffice.grpc.Client;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.tokens.AddTokenRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.tokens.DeleteTokenRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.tokens.ListTokensRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.tokens.ListTokensResponse;

import javax.validation.Valid;

@ExecuteOn(TaskExecutors.IO)
@Controller("tokens")
public class TokensController {
    @Inject
    Client client;

    @Post("add")
    public HttpResponse<?> add(@Valid @Body AddTokenRequest request){
        client.addToken(request);
        return HttpResponse.ok();
    }

    @Post("delete")
    public HttpResponse<?> delete(@Valid @Body DeleteTokenRequest request){
        client.deleteToken(request);
        return HttpResponse.ok();
    }

    @Post("list")
    public HttpResponse<ListTokensResponse> list(@Valid @Body ListTokensRequest request){
        return HttpResponse.ok(ListTokensResponse.fromModel(client.listTokens(request)));
    }


}
