package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public interface AuthenticateService {
    Subject authenticate(Credentials credentials);
}
