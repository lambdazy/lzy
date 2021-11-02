package ru.yandex.cloud.ml.platform.lzy.server;

public enum Permissions {
    BACKOFFICE_INTERNAL("backoffice.internal"),
    USERS_CREATE("backoffice.users.create"),
    USERS_DELETE("backoffice.users.delete");

    public final String name;

    Permissions(String name) {
        this.name = name;
    }
}
