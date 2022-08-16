package ai.lzy.iam.config;

public final class IamClientConfiguration {
    private String address;
    private String internalUserName;
    private String internalUserPrivateKey;

    public String address() {
        return address;
    }

    public String internalUserName() {
        return internalUserName;
    }

    public String internalUserPrivateKey() {
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
