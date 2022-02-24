package ru.yandex.cloud.ml.platform.lzy.model.channel;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

public interface Channel {
    String name();

    DataSchema contentType();
}
