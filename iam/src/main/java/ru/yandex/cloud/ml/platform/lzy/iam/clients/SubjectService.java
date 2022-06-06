package ru.yandex.cloud.ml.platform.lzy.iam.clients;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.credentials.SubjectCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

import java.util.function.Supplier;

public interface SubjectService {

    SubjectService withToken(Supplier<Credentials> tokenSupplier);

    Subject createSubject(String id, String authProvider, String providerSubjectId) throws AuthException;

    Subject subject(String id) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    void addCredentials(Subject subject, String name, String value, String type) throws AuthException;

    SubjectCredentials credentials(Subject subject, String name) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;
}
