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
            AuthPermission.INTERNAL_AUTHORIZE
    )),
    LZY_SERVANT("lzy.internal.servant", Set.of(
            AuthPermission.WORKFLOW_GET,
            AuthPermission.WORKFLOW_RUN,
            AuthPermission.WHITEBOARD_GET,
            AuthPermission.WHITEBOARD_CREATE,
            AuthPermission.WHITEBOARD_UPDATE,
            AuthPermission.INTERNAL_AUTHORIZE
    ))
    ;

    private final String role;

    private final Set<AuthPermission> permissions;

    Role(String role, Set<AuthPermission> permissions) {
        this.role = role;
        this.permissions = permissions;
    }

    public static Stream<Role> rolesByPermission(AuthPermission permission) {
        List<Role> roles = new ArrayList<>();
        for (final Role role : Role.values()) {
            if (role.permissions().contains(permission)) {
                roles.add(role);
            }
        }
        return roles.stream();
    }

    public String role() {
        return role;
    }

    public Set<AuthPermission> permissions() {
        return permissions;
    }
}
