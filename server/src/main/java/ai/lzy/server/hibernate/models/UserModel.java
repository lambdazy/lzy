package ai.lzy.server.hibernate.models;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.*;
import ai.lzy.server.hibernate.UserVerificationType;

@Entity
@Table(name = "users")
public class UserModel {

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
    @Column(name = "auth_provider")
    private String authProvider;
    @Column(name = "provider_user_id")
    private String providerUserId;
    @Column(name = "access_key")
    private String accessKey;
    @Column(name = "secret_key")
    private String secretKey;
    @Column(name = "service_account_id")
    private String serviceAccountId;
    @Column(name = "bucket")
    private String bucket;
    @Enumerated(EnumType.STRING)
    @Column(name = "access_type")
    private UserVerificationType accessType;

    public UserModel(String userId, String bucket, UserVerificationType accessType) {
        this.userId = userId;
        this.bucket = bucket;
        this.accessType = accessType;
    }

    public UserModel() {
    }

    public Set<BackofficeSessionModel> getSessions() {
        return sessions;
    }

    public String getServiceAccountId() {
        return serviceAccountId;
    }

    public void setServiceAccountId(String serviceAccountId) {
        this.serviceAccountId = serviceAccountId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public AuthType getAuthProviderEnum() {
        return AuthType.fromString(authProvider);
    }

    public void setAuthProviderEnum(AuthType authProvider) {
        this.authProvider = authProvider.toString();
    }

    public String getAuthProvider() {
        return authProvider;
    }

    public void setAuthProvider(String authProvider) {
        this.authProvider = authProvider;
    }

    public String getProviderUserId() {
        return providerUserId;
    }

    public void setProviderUserId(String providerUserId) {
        this.providerUserId = providerUserId;
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

    public Set<PublicKeyModel> getPublicKeys() {
        return publicKeys;
    }

    public Set<UserRoleModel> getRoles() {
        return roles;
    }

    public void setRoles(Set<UserRoleModel> roles) {
        this.roles = roles;
    }

    public UserVerificationType getAccessType() {
        return accessType;
    }

    public void setAccessType(UserVerificationType accessType) {
        this.accessType = accessType;
    }
}
