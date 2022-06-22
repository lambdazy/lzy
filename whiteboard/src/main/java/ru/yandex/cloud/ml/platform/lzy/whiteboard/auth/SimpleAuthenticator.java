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

    public SimpleAuthenticator(LzyServerGrpc.LzyServerBlockingStub server) {
        this.server = server;
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
}
