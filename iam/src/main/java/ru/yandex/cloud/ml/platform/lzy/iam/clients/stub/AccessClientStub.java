package ru.yandex.cloud.ml.platform.lzy.iam.clients.stub;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

import java.util.function.Supplier;

public class AccessClientStub implements AccessClient {
    @Override
    public AccessClient withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public boolean hasResourcePermission(Subject subject, AuthResource resourceId, AuthPermission permission) throws AuthException {
        return true;
    }
}
