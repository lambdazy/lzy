package ai.lzy.iam.storage.impl;

import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Servant;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.storage.db.IamDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.utils.CredentialsHelper;

import java.io.StringReader;
import java.sql.SQLException;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbAuthService implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(DbAuthService.class);

    @Inject
    private IamDataSource storage;

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        if (credentials instanceof JwtCredentials) {
            String subjectId = CredentialsHelper.issuerFromJWT(credentials.token());
            try (var conn = storage.connect()) {
                var st = conn.prepareStatement("""
                    SELECT c.name AS cred_name, c."value" AS cred_value, u.user_type AS user_type
                    FROM credentials AS c
                    JOIN users AS u
                      ON c.user_id = u.user_id
                    WHERE u.user_id = ? AND c.type = ?
                    """);

                int parameterIndex = 0;
                st.setString(++parameterIndex, subjectId);
                st.setString(++parameterIndex, credentials.type());
                var rs = st.executeQuery();
                while (rs.next()) {
                    try (StringReader keyReader = new StringReader(rs.getString("cred_value"))) {
                        if (CredentialsHelper.checkJWT(keyReader, credentials.token(), subjectId)) {
                            var subjectType = SubjectType.valueOf(rs.getString("user_type"));
                            var subject = switch (subjectType) {
                                case USER -> new User(subjectId);
                                case SERVANT -> new Servant(subjectId);
                            };
                            LOG.info("Successfully checked user::{} token with key name {}",
                                subjectId, rs.getString("cred_name"));
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
        } else {
            throw new AuthUnauthenticatedException("Failed to authenticate. Unknown credentials type.");
        }
    }
}
