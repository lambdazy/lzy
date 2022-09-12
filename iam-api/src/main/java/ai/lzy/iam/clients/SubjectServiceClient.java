package ai.lzy.iam.clients;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static java.time.Instant.now;

public interface SubjectServiceClient {

    SubjectServiceClient withToken(Supplier<Credentials> tokenSupplier);

    Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type,
                          SubjectCredentials... credentials) throws AuthException;

    Subject getSubject(String id) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    default void addCredentials(Subject subject, String name, String value, CredentialsType type,
                                @Nullable Duration ttl) throws AuthException
    {
        addCredentials(subject, new SubjectCredentials(name, value, type, ttl != null ? now().plus(ttl) : null));
    }

    void addCredentials(Subject subject, SubjectCredentials credentials) throws AuthException;

    List<SubjectCredentials> listCredentials(Subject subject) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;
}
