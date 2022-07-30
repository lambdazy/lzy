package ai.lzy.iam.resources.impl;

import ai.lzy.iam.resources.AuthResource;

public class Root implements AuthResource {

    private static final String resourceId = "lzy.resource.root";

    public static final String TYPE = "root";
    public static final Root INSTANCE = new Root();

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
