package ai.lzy.iam.clients.stub;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Servant;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.User;

import java.util.List;
import java.util.function.Supplier;

public class SubjectServiceClientStub implements SubjectServiceClient {
    @Override
    public SubjectServiceClient withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public Subject createSubject(String id, String authProvider, String providerSubjectId, SubjectType type)
            throws AuthException {
        return switch (type) {
            case USER -> new User(id);
            case SERVANT -> new Servant(id);
        };
    }

    @Override
    public Subject getSubject(String id) throws AuthException {
        return new User(id);
    }

    @Override
    public void removeSubject(Subject subject) throws AuthException {
    }

    @Override
    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
    }

    @Override
    public List<SubjectCredentials> listCredentials(Subject subject) throws AuthException {
        return List.of();
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {

    }
}
