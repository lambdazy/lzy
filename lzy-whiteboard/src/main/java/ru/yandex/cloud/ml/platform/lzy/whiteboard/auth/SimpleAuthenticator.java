package ru.yandex.cloud.ml.platform.lzy.whiteboard.auth;

import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import static ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions.WHITEBOARD_ALL;

public class SimpleAuthenticator implements Authenticator {
    private final LzyServerGrpc.LzyServerBlockingStub server;

    public SimpleAuthenticator(LzyServerGrpc.LzyServerBlockingStub server) {
        this.server = server;
    }
    @Override
    public boolean checkPermissions(IAM.Auth auth) {
        Lzy.CheckUserPermissionsResponse response = server.checkUserPermissions(
                Lzy.CheckUserPermissionsRequest
                        .newBuilder()
                        .setAuth(auth)
                        .addPermissions(WHITEBOARD_ALL.name)
                        .build()
        );
        return response.getIsOk();
    }
}
