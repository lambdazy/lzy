package ai.lzy.site.models.keys;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.site.models.UserCredentials;
import ai.lzy.v1.BackOffice;
import ai.lzy.v1.IAM;

@Introspected
public class AddPublicKeyRequest {

    private UserCredentials userCredentials;
    private String publicKey;
    private String keyName;

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public UserCredentials getUserCredentials() {
        return userCredentials;
    }

    public void setUserCredentials(UserCredentials userCredentials) {
        this.userCredentials = userCredentials;
    }

    public BackOffice.AddKeyRequest toModel(IAM.UserCredentials credentials) {
        return BackOffice.AddKeyRequest.newBuilder()
            .setPublicKey(publicKey)
            .setUserCredentials(userCredentials.toModel())
            .setBackofficeCredentials(credentials)
            .setKeyName(keyName)
            .build();
    }
}
