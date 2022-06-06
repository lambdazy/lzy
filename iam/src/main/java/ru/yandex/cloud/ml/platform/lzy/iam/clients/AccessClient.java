package ru.yandex.cloud.ml.platform.lzy.iam.clients;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

import java.util.function.Supplier;

public interface AccessClient {

    AccessClient withToken(Supplier<Credentials> tokenSupplier);

    boolean hasResourcePermission(Subject subject, AuthResource resourceId, AuthPermission permission) throws AuthException;

}
