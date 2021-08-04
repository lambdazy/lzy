package ru.yandex.cloud.ml.platform.lzy.model.data;

import java.util.List;
import java.util.Map;

public interface DataSchema {
    DataSchema[] parents();
    Map<String, Class> properties();
    List<String> canonicalOrder();

    boolean check(DataPage page);
    boolean check(DataPage.Item item);

    boolean isAssignableFrom(DataSchema contentType);
}
