package ai.lzy.iam.config;

import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;

public final class IamClientConfiguration {
    private String address;
    private String internalUserName;
    private String internalUserPrivateKey;
    private boolean enabled = true;

    public JwtCredentials createCredentials() {
        if (!enabled)
            return null;
        return JwtUtils.credentials(internalUserName, internalUserPrivateKey);
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

    public boolean isEnabled() {
        return enabled;
    }

    public IamClientConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
}
