package ru.yandex.cloud.ml.platform.lzy.model.data.types;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

public class PlainTextFileSchema implements DataSchema {
    @Override
    public String typeDescription() {
        return "";
    }

    @Override
    public SchemeType typeOfScheme() {
        return SchemeType.plain;
    }
}
