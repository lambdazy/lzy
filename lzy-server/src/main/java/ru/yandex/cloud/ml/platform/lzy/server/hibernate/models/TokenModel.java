package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "tokens")
@IdClass(TokenModel.TokenPk.class)
public class TokenModel {
    @Id
    @Column(name="name")
    private String name;

    @Id
    @Column(name = "user_id")
    private String userId;

    @Column(name = "value")
    private String value;

    @ManyToOne()
    @JoinColumn(name="user_id", nullable=false, insertable = false, updatable = false)
    private UserModel user;

    public String getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public UserModel getUser() {
        return user;
    }

    public void setUser(UserModel user) {
        this.user = user;
    }

    public TokenModel(String name, String value, UserModel user) {
        this.name = name;
        this.value = value;
        this.userId = user.getUserId();
    }

    public TokenModel(String name, String value, String userId) {
        this.name = name;
        this.value = value;
        this.userId = userId;
    }

    public TokenModel() {}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;
        TokenModel token = (TokenModel) o;
        return value.equals(token.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    public static class TokenPk implements Serializable{
        protected String name;
        protected String userId;

        public TokenPk(String name, String userId) {
            this.name = name;
            this.userId = userId;
        }

        public TokenPk() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TokenPk tokenPk = (TokenPk) o;
            return name.equals(tokenPk.name) && userId.equals(tokenPk.userId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, userId);
        }
    }
}
