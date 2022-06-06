package ru.yandex.cloud.ml.platform.lzy.iam.resources.impl;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

public record Workflow(String resourceId) implements AuthResource {

    public static final String TYPE = "workflow";

    @Override
    public String type() {
        return TYPE;
    }
}
