package ai.lzy.server.hibernate.models;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.persistence.*;

@Entity
@Table(name = "permissions")
public class PermissionModel {

    @ManyToMany()
    @JoinTable(
        name = "permission_to_role",
        joinColumns = {@JoinColumn(name = "permission_id")},
        inverseJoinColumns = {@JoinColumn(name = "role_id")}
    )
    Set<UserRoleModel> roles = new HashSet<>();
    @Id
    @Column(name = "name")
    private String name;

    public PermissionModel(String name) {
        this.name = name;
    }

    public PermissionModel() {
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PermissionModel that = (PermissionModel) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
