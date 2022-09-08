package ai.lzy.iam.clients;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.iam.resources.subjects.Subject;

import java.util.List;
import java.util.function.Supplier;

public interface SubjectServiceClient {

    SubjectServiceClient withToken(Supplier<Credentials> tokenSupplier);

    Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type,
                          List<SubjectCredentials> credentials) throws AuthException;

    default Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type)
        throws AuthException
    {
        return createSubject(authProvider, providerSubjectId, type, List.of());
    }

    Subject getSubject(String id) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    void addCredentials(Subject subject, String name, String value, CredentialsType type) throws AuthException;

    List<SubjectCredentials> listCredentials(Subject subject) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;
}
