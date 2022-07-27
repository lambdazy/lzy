package ai.lzy.backoffice.models.auth;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.BackOffice;
import ai.lzy.v1.IAM;

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

    public BackOffice.CheckPermissionRequest toModel(IAM.UserCredentials creds) {
        return BackOffice.CheckPermissionRequest.newBuilder()
            .setPermissionName(permissionName)
            .setCredentials(credentials.toModel())
            .setBackofficeCredentials(creds)
            .build();
    }
}
