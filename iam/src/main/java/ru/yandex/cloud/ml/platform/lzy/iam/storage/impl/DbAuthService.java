package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthInternalException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthPermissionDeniedException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthUnauthenticatedException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.User;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.CredentialsHelper;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
@Requires(beans = DbStorage.class)
public class DbAuthService implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(DbAuthService.class);

    @Inject
    DbStorage storage;

    @Override
    public Subject authenticate(Credentials credentials) {
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
                            LOG.info("Successfully checked user token " + subject.id() + " with key name "
                                    + rs.getString("name"));
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
