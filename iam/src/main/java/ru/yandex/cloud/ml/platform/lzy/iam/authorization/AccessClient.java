package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public interface AccessClient {

    boolean hasResourcePermission(Subject subject, String resourceId, AuthPermission permission) throws AuthException;

}
