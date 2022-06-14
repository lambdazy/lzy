package ru.yandex.cloud.ml.platform.lzy.model.data;

import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.data.types.CloudpickledPythonClassSchema;
import ru.yandex.cloud.ml.platform.lzy.model.data.types.PlainTextFileSchema;
import ru.yandex.cloud.ml.platform.lzy.model.data.types.ProtoSchema;
import ru.yandex.cloud.ml.platform.lzy.model.data.types.SchemeType;

public interface DataSchema {
    @Nullable
    String typeDescription();

    SchemeType typeOfScheme();

    static DataSchema buildDataSchema(String dataSchemeType, String typeDescription) {
        DataSchema data;
        if (SchemeType.cloudpickle.getName().equals(dataSchemeType)) {
            data = new CloudpickledPythonClassSchema(typeDescription);
        } else if (SchemeType.plain.getName().equals(dataSchemeType)) {
            data = new PlainTextFileSchema();
        } else if (SchemeType.proto.getName().equals(dataSchemeType)) {
            data = new ProtoSchema(typeDescription);
        } else {
            throw new IllegalArgumentException("provided bad dataSchemeType: " + dataSchemeType);
        }
        return data;
    }
}