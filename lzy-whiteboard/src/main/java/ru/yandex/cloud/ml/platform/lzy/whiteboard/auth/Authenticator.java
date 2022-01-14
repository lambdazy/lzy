package ru.yandex.cloud.ml.platform.lzy.whiteboard.auth;

import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

public interface Authenticator {
    boolean checkPermissions(IAM.Auth auth, Permissions permissions);
    boolean checkPermissions(LzyWhiteboard.BackofficeCredentials backoffice, Permissions permissions);
}
