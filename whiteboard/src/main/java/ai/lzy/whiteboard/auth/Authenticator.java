package ai.lzy.whiteboard.auth;

import ai.lzy.model.utils.Permissions;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

public interface Authenticator {
    boolean checkPermissions(IAM.Auth auth, Permissions permissions);
}
