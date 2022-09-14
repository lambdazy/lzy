package ai.lzy.iam.config;

import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;

public final class IamClientConfiguration {
    private String address;
    private String internalUserName;
    private String internalUserPrivateKey;

    public JwtCredentials createCredentials() {
        return JwtUtils.credentials(internalUserName, AuthProvider.INTERNAL.name(), JwtUtils.afterDays(7),
            internalUserPrivateKey);
    }

    public String getAddress() {
        return address;
    }

    public String getInternalUserName() {
        return internalUserName;
    }

    public String getInternalUserPrivateKey() {
        return internalUserPrivateKey;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setInternalUserName(String internalUserName) {
        this.internalUserName = internalUserName;
    }

    public void setInternalUserPrivateKey(String internalUserPrivateKey) {
        this.internalUserPrivateKey = internalUserPrivateKey;
    }
}
