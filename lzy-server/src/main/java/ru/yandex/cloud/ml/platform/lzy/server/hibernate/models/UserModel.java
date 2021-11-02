package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.Set;

@Entity
@Table(name = "users")
public class UserModel {

    public UserModel(String userId) {
        this.userId = userId;
    }

    @Id
    @Column(name = "user_id", nullable = false)
    private String userId;

    @OneToMany(mappedBy = "owner")
    private Set<TaskModel> tasks;

    @OneToMany(mappedBy = "user")
    private Set<TokenModel> tokens;

    public UserModel() {}

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Set<TaskModel> getTasks() {
        return tasks;
    }

    public Set<TokenModel> getTokens() {
        return tokens;
    }
}
