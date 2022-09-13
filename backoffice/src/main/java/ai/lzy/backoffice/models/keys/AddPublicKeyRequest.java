package ai.lzy.backoffice.models.keys;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;

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

    public BackOffice.AddKeyRequest toModel(LzyAuth.UserCredentials credentials) {
        return BackOffice.AddKeyRequest.newBuilder()
            .setPublicKey(publicKey)
            .setUserCredentials(userCredentials.toModel())
            .setBackofficeCredentials(credentials)
            .setKeyName(keyName)
            .build();
    }
}
