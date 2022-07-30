package ai.lzy.server.hibernate.models;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

@Entity
@Table(name = "roles")
public class UserRoleModel {

    @Id
    @Column(name = "name")
    private String roleName;

    @ManyToMany()
    @JoinTable(
        name = "role_to_user",
        joinColumns = {@JoinColumn(name = "role_id")},
        inverseJoinColumns = {@JoinColumn(name = "user_id")}
    )
    private Set<UserModel> users = new HashSet<>();

    @ManyToMany(mappedBy = "roles")
    private Set<PermissionModel> permissions = new HashSet<>();

    public UserRoleModel(String roleName) {
        this.roleName = roleName;
    }

    public UserRoleModel() {
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public Set<UserModel> getUsers() {
        return users;
    }

    public void setUsers(Set<UserModel> users) {
        this.users = users;
    }

    ;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserRoleModel that = (UserRoleModel) o;
        return roleName.equals(that.roleName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleName);
    }
}
