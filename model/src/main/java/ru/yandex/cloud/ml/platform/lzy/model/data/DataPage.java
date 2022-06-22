package ru.yandex.cloud.ml.platform.lzy.model.data;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface DataPage {
    URI id();

    List<Item> contents();

    DataSchema schema();

    interface Item extends Map<String, Object> {
        UUID id();

        Item[] dependsOn();

        DataPage page();
    }
}
