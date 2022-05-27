package ru.yandex.cloud.ml.platform.lzy.iam.resources.impl;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

public class Root implements AuthResource {

    public static final String TYPE = "root";
    private static final String resourceId = "lzy.resource.root";

    public Root() {
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
