package ru.yandex.cloud.ml.platform.lzy.backoffice.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import javax.validation.Valid;
import ru.yandex.cloud.ml.platform.lzy.backoffice.grpc.Client;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.whiteboards.WhiteboardsCommand;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.whiteboards.WhiteboardsResponse;

@ExecuteOn(TaskExecutors.IO)
@Controller("whiteboards")
public class WhiteboardsController {

    @Inject
    Client client;

    @Post("get")
    public HttpResponse<WhiteboardsResponse> get(@Valid @Body WhiteboardsCommand request) {
        return HttpResponse.ok(
            WhiteboardsResponse.fromModel(client.getWhiteboards(request))
        );
    }
}
