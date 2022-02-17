package ru.yandex.cloud.ml.platform.lzy.iam.storage;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;

public interface PermissionStorage {

    boolean hasResourcePermission(String userId, String resourceId, AuthPermission permission);

}
