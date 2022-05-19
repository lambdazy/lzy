package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.credentials.UserCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public interface SubjectService {

    Subject createSubject(String id, String authProvider, String providerUserName) throws AuthException;

    Subject subject(String id) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    void addCredentials(Subject subject, String name, String value, String type) throws AuthException;

    UserCredentials credentials(Subject subject, String name) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;
}
