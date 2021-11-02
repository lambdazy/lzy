package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "tokens", uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "value" }) })
public class TokenModel {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name="name")
    private String name;

    @Column(name = "value")
    private String value;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name="user_id", nullable=false)
    private UserModel user;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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
        this.user = user;
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
}
