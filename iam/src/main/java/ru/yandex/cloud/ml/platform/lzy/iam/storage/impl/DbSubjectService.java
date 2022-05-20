package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthBadRequestException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthInternalException;
import ru.yandex.cloud.ml.platform.lzy.iam.configs.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.credentials.SubjectCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.User;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.Storage;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.UserVerificationType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
@Requires(beans = Storage.class)
public class DbSubjectService implements SubjectService {
    private static final Logger LOG = LogManager.getLogger(DbSubjectService.class);

    @Inject
    private Storage storage;

    @Inject
    private ServiceConfig serviceConfig;

    @Override
    public Subject createSubject(String id, String authProvider, String providerSubjectId) throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "INSERT INTO users ("
                        + "user_id, "
                        + "auth_provider, "
                        + "provider_user_id, "
                        + "access_type "
                        + ") "
                        + "VALUES (?, ?, ?, ?);"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, id);
            st.setString(++parameterIndex, authProvider);
            st.setString(++parameterIndex, providerSubjectId);
            st.setString(++parameterIndex, typeForNewUser().toString());
            st.executeUpdate();
            return new User(id);
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public Subject subject(String id) throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "SELECT user_id FROM users "
                        + "WHERE user_id = ?;"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, id);
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                return new User(rs.getString("user_id"));
            } else {
                throw new AuthBadRequestException("Subject:: " + id + " NOT_FOND");
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public void removeSubject(Subject subject) throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "DELETE FROM users WHERE user_id = ?;"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "INSERT INTO credentials ("
                        + "name, "
                        + "value, "
                        + "user_id, "
                        + "type "
                        + ") "
                        + "VALUES (?, ?, ?, ?);"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, name);
            st.setString(++parameterIndex, value);
            st.setString(++parameterIndex, subject.id());
            st.setString(++parameterIndex, type);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public SubjectCredentials credentials(Subject subject, String name) throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "SELECT name, value, type FROM credentials "
                        + "WHERE user_id = ? "
                        + "AND name = ?;"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.setString(++parameterIndex, name);
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                return new SubjectCredentials(
                        rs.getString("name"),
                        rs.getString("value"),
                        rs.getString("type")
                );
            } else {
                throw new AuthBadRequestException("Credentials:: " + name + " NOT_FOND");
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "DELETE FROM credentials WHERE user_id = ? AND name = ?;"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.setString(++parameterIndex, name);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    private UserVerificationType typeForNewUser() {
        if (serviceConfig.getUserLimit() == 0) {
            return UserVerificationType.ACCESS_ALLOWED;
        }
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "SELECT count(*) from users where access_type = ?;")) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, UserVerificationType.ACCESS_ALLOWED.toString());
            final ResultSet rs = st.executeQuery();
            if (rs.next()) {
                if (rs.getInt(1) < serviceConfig.getUserLimit()) {
                    return UserVerificationType.ACCESS_ALLOWED;
                }
            } else {
                throw new AuthInternalException("Unknown user count");
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
        return UserVerificationType.ACCESS_PENDING;
    }
}
