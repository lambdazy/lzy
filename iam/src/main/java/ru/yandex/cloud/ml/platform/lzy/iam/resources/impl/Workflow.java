package ru.yandex.cloud.ml.platform.lzy.iam.resources.impl;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

public class Workflow implements AuthResource {

    public static final String TYPE = "workflow";
    private final String resourceId;

    public Workflow(String resourceId) {
        this.resourceId = resourceId;
    }

    @Override
    public String resourceId() {
        return resourceId;
    }

    @Override
    public String type() {
        return TYPE;
    }
}
