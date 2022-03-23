package ru.yandex.cloud.ml.platform.lzy.iam.resources.impl;

import ru.yandex.cloud.ml.platform.lzy.model.iam.AuthResource;

public class Workflow implements AuthResource {

    private final String type;
    private final String resourceId;

    public Workflow(String type, String resourceId) {
        this.type = type;
        this.resourceId = resourceId;
    }

    @Override
    public String resourceId() {
        return resourceId;
    }

    @Override
    public String type() {
        return type;
    }
}
