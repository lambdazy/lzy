package ru.yandex.cloud.ml.platform.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

import java.util.UUID;

public interface Channel {
    String name();
    DataSchema contentType();
}
