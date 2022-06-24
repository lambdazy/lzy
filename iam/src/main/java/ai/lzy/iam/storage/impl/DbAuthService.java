package ai.lzy.iam.storage.impl;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.authorization.exceptions.AuthInternalException;
import ai.lzy.iam.authorization.exceptions.AuthPermissionDeniedException;
import ai.lzy.iam.authorization.exceptions.AuthUnauthenticatedException;
import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.User;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.storage.Storage;
import ai.lzy.iam.utils.CredentialsHelper;

import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
@Requires(beans = Storage.class)
public class DbAuthService implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(DbAuthService.class);

    @Inject
    private Storage storage;

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        Subject subject;
        if (credentials instanceof JwtCredentials) {
            subject = new User(CredentialsHelper.issuerFromJWT(((JwtCredentials) credentials).token()));
            try (final PreparedStatement st = storage.connect().prepareStatement(
                    "SELECT * FROM credentials "
                            + "WHERE user_id = ? AND type = ?;"
            )) {
                int parameterIndex = 0;
                st.setString(++parameterIndex, subject.id());
                st.setString(++parameterIndex, credentials.type());
                final ResultSet rs = st.executeQuery();
                while (rs.next()) {
                    try (StringReader keyReader = new StringReader(rs.getString("value"))) {
                        if (CredentialsHelper.checkJWT(keyReader,
                                ((JwtCredentials) credentials).token(),
                                subject.id())) {
                            LOG.info("Successfully checked user::{} token with key name {}",
                                    subject.id(), rs.getString("name"));
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
