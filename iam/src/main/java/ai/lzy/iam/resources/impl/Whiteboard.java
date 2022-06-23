package ai.lzy.iam.resources.impl;

import ai.lzy.iam.resources.AuthResource;

public record Whiteboard(String resourceId) implements AuthResource {

    public static final String TYPE = "whiteboard";

    @Override
    public String type() {
        return TYPE;
    }
}
