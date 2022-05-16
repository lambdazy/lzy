package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;

public interface AccessClient {

    boolean hasResourcePermission(String userId, String resourceId, AuthPermission permission) throws AuthException;

}
