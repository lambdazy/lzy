package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Entity
@Table(name = "permissions")
public class PermissionModel {

    @Id
    @Column(name = "name")
    private String name;

    @ManyToMany()
    @JoinTable(
            name="permission_to_role",
            joinColumns = {@JoinColumn(name = "permission_id")},
            inverseJoinColumns = {@JoinColumn(name = "role_id")}
    )
    Set<UserRoleModel> roles = new HashSet<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<UserRoleModel> getRoles() {
        return roles;
    }

    public void setRoles(Set<UserRoleModel> roles) {
        this.roles = roles;
    }

    public PermissionModel(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PermissionModel that = (PermissionModel) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public PermissionModel() {}
}
