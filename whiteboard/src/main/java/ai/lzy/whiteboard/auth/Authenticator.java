package ai.lzy.whiteboard.auth;

import ai.lzy.model.utils.Permissions;
import ai.lzy.priv.v2.IAM;

public interface Authenticator {
    boolean checkPermissions(IAM.Auth auth, Permissions permissions);
}
