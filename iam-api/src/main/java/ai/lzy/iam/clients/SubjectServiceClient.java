package ai.lzy.iam.clients;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.iam.resources.subjects.Subject;

import java.util.List;
import java.util.function.Supplier;

public interface SubjectServiceClient {

    SubjectServiceClient withToken(Supplier<Credentials> tokenSupplier);

    Subject createSubject(String id, String authProvider, String providerSubjectId, SubjectType type)
            throws AuthException;

    Subject getSubject(String id) throws AuthException;

    void removeSubject(Subject subject) throws AuthException;

    void addCredentials(Subject subject, String name, String value, String type) throws AuthException;

    List<SubjectCredentials> listCredentials(Subject subject) throws AuthException;

    void removeCredentials(Subject subject, String name) throws AuthException;
}
