package ru.yandex.cloud.ml.platform.lzy.model.channel;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

public interface ChannelSpec {
    String name();

    DataSchema contentType();
}
