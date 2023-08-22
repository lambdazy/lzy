package ai.lzy.iam.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public enum Role {

    LZY_WORKFLOW_OWNER("lzy.workflow.owner", Set.of(
        AuthPermission.WORKFLOW_GET,
        AuthPermission.WORKFLOW_STOP,
        AuthPermission.WORKFLOW_RUN,
        AuthPermission.WORKFLOW_DELETE
    )),
    LZY_WHITEBOARD_OWNER("lzy.whiteboard.owner", Set.of(
        AuthPermission.WHITEBOARD_GET,
        AuthPermission.WHITEBOARD_CREATE,
        AuthPermission.WHITEBOARD_UPDATE,
        AuthPermission.WHITEBOARD_DELETE
    )),
    LZY_WHITEBOARD_READER("lzy.whiteboard.reader", Set.of(
        AuthPermission.WHITEBOARD_GET
    )),
    LZY_INTERNAL_USER("lzy.internal.user", Set.of(
        AuthPermission.WHITEBOARD_GET,
        AuthPermission.WHITEBOARD_CREATE,
        AuthPermission.WHITEBOARD_UPDATE,
        AuthPermission.WHITEBOARD_DELETE,
        AuthPermission.WORKFLOW_GET,
        AuthPermission.WORKFLOW_STOP,
        AuthPermission.WORKFLOW_RUN,
        AuthPermission.WORKFLOW_DELETE,
        AuthPermission.WORKFLOW_MANAGE,
        AuthPermission.INTERNAL_AUTHORIZE
    )),
    LZY_WORKER("lzy.internal.worker", Set.of(
        AuthPermission.WORKFLOW_GET,
        AuthPermission.WORKFLOW_RUN,
        AuthPermission.WHITEBOARD_GET,
        AuthPermission.WHITEBOARD_UPDATE,
        AuthPermission.INTERNAL_AUTHORIZE
    )),
    LZY_INTERNAL_ADMIN("lzy.internal.admin", Set.of(
        AuthPermission.INTERNAL_UPDATE_IMAGES,
        AuthPermission.INTERNAL_AUTHORIZE
    ))
    ;

    private static final Role[] ALL = Role.values();

    private final String value;
    private final Set<AuthPermission> permissions;

    Role(String value, Set<AuthPermission> permissions) {
        this.value = value;
        this.permissions = permissions;
    }

    public static Role of(String roleValue) {
        for (var role : ALL) {
            if (role.value().equals(roleValue)) {
                return role;
            }
        }
        throw new IllegalArgumentException("Unexpected role " + roleValue);
    }

    public static Stream<Role> rolesByPermission(AuthPermission permission) {
        List<Role> roles = new ArrayList<>();
        for (final Role role : ALL) {
            if (role.permissions().contains(permission)) {
                roles.add(role);
            }
        }
        return roles.stream();
    }

    public String value() {
        return value;
    }

    public Set<AuthPermission> permissions() {
        return permissions;
    }

}
