package ai.lzy.iam.storage.impl;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.authorization.exceptions.AuthInternalException;
import ai.lzy.iam.authorization.exceptions.AuthPermissionDeniedException;
import ai.lzy.iam.authorization.exceptions.AuthUnauthenticatedException;
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
                    SELECT value, name, user_type
                    FROM credentials LEFT JOIN users
                    WHERE user_id = ? AND type = ?"""
                );

                int parameterIndex = 0;
                st.setString(++parameterIndex, subjectId);
                st.setString(++parameterIndex, credentials.type());
                var rs = st.executeQuery();
                while (rs.next()) {
                    try (StringReader keyReader = new StringReader(rs.getString("value"))) {
                        if (CredentialsHelper.checkJWT(keyReader, credentials.token(), subjectId)) {
                            SubjectType subjectType = SubjectType.valueOf(rs.getString("user_type"));
                            Subject subject = switch (subjectType) {
                                case USER -> new User(subjectId);
                                case SERVANT -> new Servant(subjectId);
                            };
                            LOG.info("Successfully checked user::{} token with key name {}",
                                    subjectId, rs.getString("name"));
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
