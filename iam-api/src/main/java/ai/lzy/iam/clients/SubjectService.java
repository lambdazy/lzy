package ai.lzy.iam.clients;

import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.iam.resources.subjects.Subject;

import java.util.function.Supplier;

public interface SubjectService {

    SubjectService withToken(Supplier<Credentials> tokenSupplier);

    Subject createSubject(Subject subject, String authProvider, String providerSubjectId) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    void addCredentials(Subject subject, String name, String value, String type) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;
}
