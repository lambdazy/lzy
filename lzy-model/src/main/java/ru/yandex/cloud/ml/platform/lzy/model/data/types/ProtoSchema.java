package ru.yandex.cloud.ml.platform.lzy.model.data.types;

import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;


public class ProtoSchema implements DataSchema {
    private final String protoDeclaration;

    public ProtoSchema(String protoDeclaration) {
        this.protoDeclaration = protoDeclaration;
    }

    @Nullable
    @Override
    public String typeDescription() {
        return protoDeclaration;
    }

    @Override
    public SchemeType typeOfScheme() {
        return SchemeType.proto;
    }
}
