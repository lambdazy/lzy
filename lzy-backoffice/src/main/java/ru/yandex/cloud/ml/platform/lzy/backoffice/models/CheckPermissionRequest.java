package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;


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

    public BackOffice.CheckPermissionRequest toModel(IAM.UserCredentials creds){
        return BackOffice.CheckPermissionRequest.newBuilder()
            .setPermissionName(permissionName)
            .setCredentials(credentials.toModel())
            .setBackofficeCredentials(creds)
            .build();
    }
}
