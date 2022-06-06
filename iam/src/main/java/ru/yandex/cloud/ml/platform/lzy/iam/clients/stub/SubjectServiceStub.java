package ru.yandex.cloud.ml.platform.lzy.iam.clients.stub;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.credentials.SubjectCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.User;

import java.util.function.Supplier;

public class SubjectServiceStub implements SubjectService {
    @Override
    public SubjectService withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public Subject createSubject(String id, String authProvider, String providerSubjectId) throws AuthException {
        return new User(id);
    }

    @Override
    public Subject subject(String id) throws AuthException {
        return new User(id);
    }

    @Override
    public void removeSubject(Subject subject) throws AuthException {
    }

    @Override
    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
    }

    @Override
    public SubjectCredentials credentials(Subject subject, String name) throws AuthException {
        return null;
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {

    }
}
