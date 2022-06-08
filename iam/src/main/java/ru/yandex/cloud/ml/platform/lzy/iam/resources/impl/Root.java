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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Root that = (Root) o;
        return this.resourceId().equals(that.resourceId()) && this.type().equals(that.type());
    }
}
