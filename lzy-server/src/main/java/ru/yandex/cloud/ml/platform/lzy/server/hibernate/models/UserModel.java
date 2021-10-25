package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "users")
public class UserModel {

    public UserModel(String userId, String publicToken) {
        this.userId = userId;
        this.publicToken = publicToken;
    }

    @Id
    @Column(name = "user_id")
    private String userId;

    @Column(name = "public_token")
    private String publicToken;

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
}
