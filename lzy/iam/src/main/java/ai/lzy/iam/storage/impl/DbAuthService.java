package ai.lzy.iam.storage.impl;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.resources.subjects.Worker;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import io.jsonwebtoken.Claims;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.StringReader;
import java.sql.SQLException;
import javax.annotation.Nonnull;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbAuthService implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(DbAuthService.class);

    @Inject
    private IamDataSource storage;

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        if (credentials instanceof JwtCredentials) {
            return authenticateJwt(credentials);
        }

        throw new AuthUnauthenticatedException("Failed to authenticate. Unknown credentials type.");
    }

    @Nonnull
    private Subject authenticateJwt(Credentials credentials) {
        var jwtPayload = JwtUtils.parseJwt(credentials.token());
        if (jwtPayload == null) {
            LOG.error("Cannot parse JWT token '{}' of type {}", credentials.token(), credentials.type());
            throw new AuthUnauthenticatedException("Failed to authenticate. Invalid JWT.");
        }

        var header = JwtUtils.getHeader(credentials.token());

        final String credName;
        if (header == null || !header.containsKey("kn")) {
            credName = null;
        } else {
            credName = (String) header.get("kn");
        }

        var providerLogin = (String) jwtPayload.get(Claims.ISSUER);
        var providerName = (String) jwtPayload.get("pvd");

        LOG.debug("Authenticate by JWT: id={}, provider={}", providerLogin, providerName);

        if (Strings.isEmpty(providerLogin) || Strings.isEmpty(providerName)) {
            LOG.error("Either providerLogin ({}) or providerName ({}) is empty. Token '{}' of type {}",
                providerLogin, providerName, credentials.token(), credentials.type());
            throw new AuthUnauthenticatedException("Failed to authenticate. Invalid JWT.");
        }

        try (var conn = storage.connect();
             var st = conn.prepareStatement("""
                SELECT
                    c.user_id AS user_id,
                    c.name AS cred_name,
                    c.value AS cred_value,
                    u.user_type AS user_type
                FROM credentials AS c
                JOIN users AS u
                  ON c.user_id = u.user_id
                WHERE u.provider_user_id = ? AND u.auth_provider = ? AND c.type = ?
                  AND (c.expired_at IS NULL OR c.expired_at > NOW()) AND (? IS NULL OR c.name = ?)
                """))
        {

            int parameterIndex = 0;
            st.setString(++parameterIndex, providerLogin);
            st.setString(++parameterIndex, providerName);
            st.setString(++parameterIndex, credentials.type());
            st.setString(++parameterIndex, credName);
            st.setString(++parameterIndex, credName);

            var rs = st.executeQuery();
            while (rs.next()) {
                // validate auth provider
                AuthProvider.valueOf(providerName);

                try (StringReader keyReader = new StringReader(rs.getString("cred_value"))) {
                    if (JwtUtils.checkJWT(keyReader, credentials.token(), providerLogin, providerName)) {
                        var subjectId = rs.getString("user_id");
                        var subjectType = SubjectType.valueOf(rs.getString("user_type"));

                        var subject = switch (subjectType) {
                            case USER -> new User(subjectId);
                            case WORKER -> new Worker(subjectId);
                        };

                        LOG.info("Successfully checked {}::{} token with key name {}",
                            subjectType, subjectId, rs.getString("cred_name"));
                        return subject;
                    }
                } catch (Exception e) {
                    throw new AuthInternalException(e);
                }
            }
            throw new AuthPermissionDeniedException("Permission denied. Not found valid key.");
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }
}
