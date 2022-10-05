package ai.lzy.backoffice.models.keys;

import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;
import io.micronaut.core.annotation.Introspected;

@Introspected
public class DeletePublicKeyRequest {

    UserCredentials credentials;
    private String keyName;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public BackOffice.DeleteKeyRequest toModel(LzyAuth.UserCredentials credentials) {
        return BackOffice.DeleteKeyRequest.newBuilder()
            .setBackofficeCredentials(credentials)
            .setCredentials(this.credentials.toModel())
            .setKeyName(this.keyName)
            .build();
    }
}
