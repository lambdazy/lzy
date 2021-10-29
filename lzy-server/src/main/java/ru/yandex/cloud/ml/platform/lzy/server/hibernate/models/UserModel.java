package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.Set;

@Entity
@Table(name = "users")
public class UserModel {

    public UserModel(String userId, String publicToken) {
        this.userId = userId;
        this.publicToken = publicToken;
    }

    @Id
    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "public_token", nullable = false)
    private String publicToken;

    @OneToMany(mappedBy = "owner")
    private Set<TaskModel> tasks;

    public UserModel() {}

    public String getPublicToken() {
        return publicToken;
    }

    public void setPublicToken(String publicToken) {
        this.publicToken = publicToken;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Set<TaskModel> getTasks() {
        return tasks;
    }
}
