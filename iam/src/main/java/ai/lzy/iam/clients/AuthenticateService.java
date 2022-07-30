package ai.lzy.iam.clients;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.resources.subjects.Subject;

public interface AuthenticateService {

    Subject authenticate(Credentials credentials) throws AuthException;

}
