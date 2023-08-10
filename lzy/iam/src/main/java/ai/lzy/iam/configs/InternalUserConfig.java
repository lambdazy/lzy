package ai.lzy.iam.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("iam.internal")
public class InternalUserConfig {
    private String userName;
    private String credentialName;
    private String credentialValue;
    private String credentialType;
    private String credentialPrivateKey;
    private String userAdminName;
    private String userAdminCredentialValue;

    public String userName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String credentialName() {
        return credentialName;
    }

    public void setCredentialName(String credentialName) {
        this.credentialName = credentialName;
    }

    public String credentialValue() {
        return credentialValue;
    }

    public void setCredentialValue(String credentialValue) {
        this.credentialValue = credentialValue;
    }

    public String credentialType() {
        return credentialType;
    }

    public void setCredentialType(String credentialType) {
        this.credentialType = credentialType;
    }

    public String credentialPrivateKey() {
        return credentialPrivateKey;
    }

    public void setCredentialPrivateKey(String credentialPrivateKey) {
        this.credentialPrivateKey = credentialPrivateKey;
    }

    public String userAdminName() {
        return userAdminName;
    }

    public void setUserAdminName(String userAdminName) {
        this.userAdminName = userAdminName;
    }

    public String userAdminCredentialValue() {
        return userAdminCredentialValue;
    }

    public void setUserAdminCredentialValue(String userAdminCredentialValue) {
        this.userAdminCredentialValue = userAdminCredentialValue;
    }
}
