package ai.lzy.iam.clients;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;

public interface SubjectServiceClient {

    SubjectServiceClient withToken(Supplier<Credentials> tokenSupplier);

    SubjectServiceClient withIdempotencyKey(String idempotencyKey);

    Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type,
                          SubjectCredentials... credentials) throws AuthException;

    Subject getSubject(String id) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    void addCredentials(Subject subject, SubjectCredentials credentials) throws AuthException;

    List<SubjectCredentials> listCredentials(Subject subject) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;

    @Nullable
    Subject findSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type) throws AuthException;
}
