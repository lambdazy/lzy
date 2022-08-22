package ai.lzy.iam.clients.stub;

import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.iam.clients.SubjectService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.User;

import java.util.function.Supplier;

public class SubjectServiceStub implements SubjectService {
    @Override
    public SubjectService withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public Subject createSubject(Subject subj, String authProvider, String providerSubjectId) throws AuthException {
        return subj;
    }

    @Override
    public void removeSubject(Subject subject) throws AuthException {
    }

    @Override
    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {

    }
}
