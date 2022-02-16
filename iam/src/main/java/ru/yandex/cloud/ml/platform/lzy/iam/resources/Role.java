package ru.yandex.cloud.ml.platform.lzy.iam.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public enum Role {
    ;

    private final String role;

    private final Set<AuthPermission> permissions;

    Role(String role, Set<AuthPermission> permissions) {
        this.role = role;
        this.permissions = permissions;
    }

    public String role() {
        return role;
    }

    public Set<AuthPermission> permissions() {
        return permissions;
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
}
