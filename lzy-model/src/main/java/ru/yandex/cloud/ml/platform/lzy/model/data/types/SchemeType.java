package ru.yandex.cloud.ml.platform.lzy.model.data.types;

public enum SchemeType {
    plain("plain"),
    proto("proto"),
    cloudpickle("cloudpickle");

    private final String description;

    SchemeType(String nameInDb) {
        description = nameInDb;
    }

    public String getName() {
        return description;
    }
}
