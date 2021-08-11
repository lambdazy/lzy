package ru.yandex.cloud.ml.platform.lzy.model.data.types;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataPage;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

import java.util.List;
import java.util.Map;

public class PlainTextFileSchema implements DataSchema {
    @Override
    public DataSchema[] parents() {
        return new DataSchema[0];
    }

    @Override
    public Map<String, Class> properties() {
        return Map.of("line", String.class);
    }

    @Override
    public List<String> canonicalOrder() {
        return List.of("line");
    }

    @Override
    public boolean check(DataPage page) {
        return true;
    }

    @Override
    public boolean check(DataPage.Item item) {
        return true;
    }

    @Override
    public boolean isAssignableFrom(DataSchema contentType) {
        return contentType.properties().containsKey("line");
    }
}
