package ai.lzy.iam.clients.stub;

import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.*;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class SubjectServiceClientStub implements SubjectServiceClient {
    @Override
    public SubjectServiceClient withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public SubjectServiceClient withIdempotencyKey(String idempotencyKey) {
        return this;
    }

    @Override
    public Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type,
                                 SubjectCredentials... credentials) throws AuthException
    {
        final var id = "user_stub_" + UUID.randomUUID();
        return switch (type) {
            case USER -> new User(id);
            case WORKER -> new Worker(id);
            case EXTERNAL -> new External(id, "");
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
    public void addCredentials(Subject subject, SubjectCredentials credentials) throws AuthException {
    }

    @Override
    public List<SubjectCredentials> listCredentials(Subject subject) throws AuthException {
        return List.of();
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {

    }

    @Nullable
    @Override
    public Subject findSubject(AuthProvider authProvider,
                               String providerSubjectId, SubjectType type) throws AuthException
    {
        return null;
    }
}
