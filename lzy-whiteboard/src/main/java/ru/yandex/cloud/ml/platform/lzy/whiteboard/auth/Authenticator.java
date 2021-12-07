package ru.yandex.cloud.ml.platform.lzy.whiteboard.auth;

import yandex.cloud.priv.datasphere.v2.lzy.IAM;

public interface Authenticator {
    boolean checkPermissions(IAM.Auth auth);
}
