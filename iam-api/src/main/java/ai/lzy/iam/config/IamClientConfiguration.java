package ai.lzy.iam.config;

public final class IamClientConfiguration {
    private String address;
    private String internalUserName;
    private String internalUserPrivateKey;

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
