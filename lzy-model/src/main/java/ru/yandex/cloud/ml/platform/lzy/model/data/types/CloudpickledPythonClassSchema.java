package ru.yandex.cloud.ml.platform.lzy.model.data.types;

import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

public class CloudpickledPythonClassSchema implements DataSchema {

    private final String content;

    public CloudpickledPythonClassSchema(String base64SerializedClass) {
        content = base64SerializedClass;
    }

    @Nullable
    @Override
    public String typeDescription() {
        return content;
    }

    @Override
    public SchemeType typeOfScheme() {
        return SchemeType.cloudpickle;
    }
}
