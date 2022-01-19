package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import ru.yandex.cloud.ml.platform.lzy.model.utils.AuthProviders;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "users")
public class UserModel {

    public UserModel(String userId, String bucket) {
        this.userId = userId;
        this.bucket = bucket;
    }

    @Id
    @Column(name = "user_id", nullable = false)
    private String userId;

    @OneToMany(mappedBy = "owner")
    private Set<TaskModel> tasks;

    @OneToMany(mappedBy = "user")
    private Set<PublicKeyModel> publicKeys;

    @ManyToMany(mappedBy = "users")
    private Set<UserRoleModel> roles = new HashSet<>();

    @OneToMany(mappedBy = "owner")
    private Set<BackofficeSessionModel> sessions;

    public Set<BackofficeSessionModel> getSessions() {
        return sessions;
    }

    @Column(name = "auth_provider")
    private String authProvider;

    @Column(name = "provider_user_id")
    private String providerUserId;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    @Column(name = "bucket")
    private String bucket;

    public AuthProviders getAuthProviderEnum() {
        return AuthProviders.fromString(authProvider);
    }

    public String getAuthProvider() {
        return authProvider;
    }

    public void setAuthProvider(String authProvider) {
        this.authProvider = authProvider;
    }

    public void setAuthProviderEnum(AuthProviders authProvider) {
        this.authProvider = authProvider.toString();
    }

    public String getProviderUserId() {
        return providerUserId;
    }

    public void setProviderUserId(String providerUserId) {
        this.providerUserId = providerUserId;
    }

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

    public Set<PublicKeyModel> getPublicKeys() {
        return publicKeys;
    }

    public Set<UserRoleModel> getRoles() {
        return roles;
    }

    public void setRoles(Set<UserRoleModel> roles) {
        this.roles = roles;
    }
}
