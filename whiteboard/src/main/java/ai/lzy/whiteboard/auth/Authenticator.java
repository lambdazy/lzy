package ai.lzy.whiteboard.auth;

import ai.lzy.model.utils.Permissions;
import ai.lzy.v1.IAM;

public interface Authenticator {
    boolean checkPermissions(IAM.Auth auth, Permissions permissions);
}
