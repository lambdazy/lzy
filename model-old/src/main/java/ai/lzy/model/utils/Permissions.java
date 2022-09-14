package ai.lzy.model.utils;

public enum Permissions {
    BACKOFFICE_INTERNAL("backoffice.internal.privateApi"),
    USERS_CREATE("backoffice.users.create"),
    USERS_DELETE("backoffice.users.delete"),
    USERS_LIST("backoffice.users.list"),
    WHITEBOARD_ALL("lzy.whiteboard.all");

    public final String name;

    Permissions(String name) {
        this.name = name;
    }
}
