package ru.yandex.cloud.ml.platform.lzy.iam.clients.stub;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public class AuthenticateServiceStub implements AuthenticateService {
    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        return () -> "StubId";
    }
}
