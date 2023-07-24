package ai.lzy.iam.resources;

public enum AuthPermission {
    // Workflow permissions
    WORKFLOW_GET("lzy.workflow.get"),
    WORKFLOW_RUN("lzy.workflow.run"),
    WORKFLOW_STOP("lzy.workflow.stop"),
    WORKFLOW_DELETE("lzy.workflow.delete"),

    // Whiteboard permissions
    WHITEBOARD_GET("lzy.whiteboard.get"),
    WHITEBOARD_CREATE("lzy.whiteboard.create"),
    WHITEBOARD_UPDATE("lzy.whiteboard.update"),
    WHITEBOARD_DELETE("lzy.whiteboard.delete"),

    // Internal permissions
    INTERNAL_AUTHORIZE("lzy.internal.authorize"),
    INTERNAL_UPDATE_IMAGES("lzy.internal.update-images")
    ;

    private final String permission;

    AuthPermission(String permission) {
        this.permission = permission;
    }

    public static AuthPermission fromString(String permission) {
        for (AuthPermission p : AuthPermission.values()) {
            if (p.permission.equalsIgnoreCase(permission)) {
                return p;
            }
        }
        return null;
    }

    public String permission() {
        return permission;
    }
}
