package ai.lzy.whiteboard.auth;

import ai.lzy.model.utils.Permissions;
import ai.lzy.priv.v2.IAM;
import ai.lzy.priv.v2.Lzy;
import ai.lzy.priv.v2.LzyServerGrpc;

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
