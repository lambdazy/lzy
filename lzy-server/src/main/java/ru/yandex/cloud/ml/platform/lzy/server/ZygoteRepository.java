package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

import java.util.stream.Stream;

public interface ZygoteRepository {
    boolean publish(String name, Zygote zygote);
    Stream<String> list();

    Zygote get(String name);
}
