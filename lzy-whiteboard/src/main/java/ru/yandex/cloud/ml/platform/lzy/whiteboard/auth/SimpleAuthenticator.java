package ru.yandex.cloud.ml.platform.lzy.whiteboard.auth;

import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

public class SimpleAuthenticator implements Authenticator {

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final LzyBackofficeGrpc.LzyBackofficeBlockingStub backoffice;

    public SimpleAuthenticator(LzyServerGrpc.LzyServerBlockingStub server,
        LzyBackofficeGrpc.LzyBackofficeBlockingStub backoffice) {
        this.server = server;
        this.backoffice = backoffice;
    }

    @Override
    public boolean checkPermissions(IAM.Auth auth, Permissions permissions) {
        Lzy.CheckUserPermissionsResponse response = server.checkUserPermissions(
            Lzy.CheckUserPermissionsRequest
                .newBuilder()
                .setAuth(auth)
                .addPermissions(permissions.name)
                .build()
        );
        return response.getIsOk();
    }

    @Override
    public boolean checkPermissions(LzyWhiteboard.BackofficeCredentials backofficeCreds, Permissions permissions) {
        BackOffice.CheckPermissionResponse response = backoffice.checkPermission(
            BackOffice.CheckPermissionRequest
                .newBuilder()
                .setBackofficeCredentials(backofficeCreds.getBackofficeCredentials())
                .setCredentials(backofficeCreds.getCredentials())
                .setPermissionName(permissions.name)
                .build()
        );
        return response.getGranted();
    }
}
