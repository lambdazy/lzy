package ru.yandex.cloud.ml.platform.lzy.backoffice;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("api")
public class HelloController {

    @Get(value = "hello", produces = MediaType.TEXT_PLAIN)
    public String index() {
        return "Hello World";
    }
}
