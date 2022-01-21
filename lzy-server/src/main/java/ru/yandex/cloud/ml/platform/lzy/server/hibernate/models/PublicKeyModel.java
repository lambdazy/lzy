package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "public_keys")
@IdClass(PublicKeyModel.PublicKeyPk.class)
public class PublicKeyModel {
    @Id
    @Column(name="name")
    private String name;

    @Id
    @Column(name = "user_id")
    private String userId;

    @Column(name = "value", length = 512)
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

    public PublicKeyModel(String name, String value, UserModel user) {
        this.name = name;
        this.value = value;
        this.userId = user.getUserId();
    }

    public PublicKeyModel(String name, String value, String userId) {
        this.name = name;
        this.value = value;
        this.userId = userId;
    }

    public PublicKeyModel() {}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;
        PublicKeyModel token = (PublicKeyModel) o;
        return value.equals(token.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    public static class PublicKeyPk implements Serializable{
        protected String name;
        protected String userId;

        public PublicKeyPk(String name, String userId) {
            this.name = name;
            this.userId = userId;
        }

        public PublicKeyPk() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PublicKeyPk tokenPk = (PublicKeyPk) o;
            return name.equals(tokenPk.name) && userId.equals(tokenPk.userId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, userId);
        }
    }
}
