package ru.yandex.cloud.ml.platform.lzy.iam.resources;

public enum AuthPermission {
    WORKFLOW_GET("lzy.workflow.get"),
    WORKFLOW_STOP("lzy.workflow.stop"),
    WORKFLOW_RUN("lzy.workflow.run"),
    WORKFLOW_DELETE("lzy.workflow.delete"),
    WHITEBOARD_GET("lzy.whiteboard.get"),
    WHITEBOARD_UPDATE("lzy.whiteboard.update"),
    WHITEBOARD_CREATE("lzy.whiteboard.create"),
    WHITEBOARD_DELETE("lzy.whiteboard.delete"),
    ;

    private final String permission;

    AuthPermission(String permission) {
        this.permission = permission;
    }

    public String permission() {
        return permission;
    }
}
