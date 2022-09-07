package ai.lzy.iam.clients.stub;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthException;
import io.jsonwebtoken.Claims;

public class AuthenticateServiceStub implements AuthenticateService {
    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        return new User((String) JwtUtils.parseJwt(credentials.token()).get(Claims.ISSUER));
    }
}
