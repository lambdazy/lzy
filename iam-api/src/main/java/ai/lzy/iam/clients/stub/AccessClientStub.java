package ai.lzy.iam.clients.stub;

import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;

import java.util.function.Supplier;

public class AccessClientStub implements AccessClient {
    @Override
    public AccessClient withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public boolean hasResourcePermission(
            Subject subject,
            AuthResource resourceId,
            AuthPermission permission) throws AuthException
    {
        return true;
    }
}
