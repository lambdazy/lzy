package ai.lzy.whiteboard.auth;

import ai.lzy.model.utils.Permissions;
import ai.lzy.v1.deprecated.LzyAuth;

public interface Authenticator {
    boolean checkPermissions(LzyAuth.Auth auth, Permissions permissions);
}
