package ai.lzy.whiteboard.auth;

import ai.lzy.model.utils.Permissions;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

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
