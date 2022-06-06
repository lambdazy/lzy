package ru.yandex.cloud.ml.platform.lzy.iam.resources.impl;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

public record Whiteboard(String resourceId) implements AuthResource {

    public static final String TYPE = "whiteboard";

    @Override
    public String type() {
        return TYPE;
    }
}
