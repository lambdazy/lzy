package ai.lzy.iam.clients.stub;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;

public class AuthenticateServiceStub implements AuthenticateService {
    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        return () -> "StubId";
    }
}
