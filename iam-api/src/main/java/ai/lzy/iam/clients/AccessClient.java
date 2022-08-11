package ai.lzy.iam.clients;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;

import java.util.function.Supplier;

public interface AccessClient {

    AccessClient withToken(Supplier<Credentials> tokenSupplier);

    boolean hasResourcePermission(
            Subject subject,
            AuthResource resourceId,
            AuthPermission permission) throws AuthException;

}
