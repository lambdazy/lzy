package ru.yandex.cloud.ml.platform.lzy.iam.clients;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public interface AuthenticateService {

    Subject authenticate(Credentials credentials) throws AuthException;

}
