package ai.lzy.iam.clients;

import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;

public interface AuthenticateService {

    Subject authenticate(Credentials credentials) throws AuthException;

}
