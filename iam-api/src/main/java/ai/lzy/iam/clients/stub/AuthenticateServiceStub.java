package ai.lzy.iam.clients.stub;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import io.jsonwebtoken.Claims;
import org.apache.logging.log4j.util.Strings;

import java.util.Map;
import java.util.Objects;

public class AuthenticateServiceStub implements AuthenticateService {
    private final Map<String, String> users;

    public AuthenticateServiceStub(Map<String, String> users) {
        this.users = users;
    }

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        var payload = JwtUtils.parseJwt(credentials.token());
        if (payload == null) {
            throw new AuthInternalException("Invalid JWT");
        }

        var login = (String) payload.get(Claims.ISSUER);
        var provider = (String) payload.get(JwtUtils.CLAIM_PROVIDER);

        if (Strings.isEmpty(login) || Strings.isEmpty(provider)) {
            throw new AuthInternalException("Invalid JWT");
        }

        var userId = Objects.requireNonNull(users.get(login + "@" + provider));

        return new User(userId);
    }
}
