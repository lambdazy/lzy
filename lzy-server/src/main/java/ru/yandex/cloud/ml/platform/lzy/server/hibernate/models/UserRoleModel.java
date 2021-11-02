package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Entity
@Table(name = "roles")
public class UserRoleModel {

    @Id
    @Column(name = "name")
    private String roleName;

    @ManyToMany()
    @JoinTable(
            name="role_to_user",
            joinColumns = {@JoinColumn(name = "role_id")},
            inverseJoinColumns = {@JoinColumn(name = "user_id")}
    )
    private Set<UserModel> users = new HashSet<>();

    @ManyToMany(mappedBy = "roles")
    private Set<PermissionModel> permissions = new HashSet<>();

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public Set<UserModel> getUsers() {
        return users;
    }

    public UserRoleModel(String roleName) {
        this.roleName = roleName;
    }

    public UserRoleModel() {};

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserRoleModel that = (UserRoleModel) o;
        return roleName.equals(that.roleName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleName);
    }
}
