package ai.lzy.backoffice.models.auth;

import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;
import io.micronaut.core.annotation.Introspected;

@Introspected
public class CheckPermissionRequest {

    private UserCredentials credentials;
    private String permissionName;

    public UserCredentials credentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public String permissionName() {
        return permissionName;
    }

    public void setPermissionName(String permissionName) {
        this.permissionName = permissionName;
    }

    public BackOffice.CheckPermissionRequest toModel(LzyAuth.UserCredentials creds) {
        return BackOffice.CheckPermissionRequest.newBuilder()
            .setPermissionName(permissionName)
            .setCredentials(credentials.toModel())
            .setBackofficeCredentials(creds)
            .build();
    }
}
