package ai.lzy.iam.storage.impl;

import ai.lzy.iam.authorization.exceptions.AuthBadRequestException;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.authorization.exceptions.AuthInternalException;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Servant;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.UserVerificationType;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbSubjectService {
    private static final Logger LOG = LogManager.getLogger(DbSubjectService.class);

    @Inject
    private IamDataSource storage;

    @Inject
    private ServiceConfig serviceConfig;

    public Subject createSubject(String id, String authProvider, String providerSubjectId, SubjectType subjectType)
            throws AuthException {
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement("""
                INSERT INTO users (
                user_id, 
                auth_provider, 
                provider_user_id, 
                access_type 
                user_type 
                ) 
                VALUES (?, ?, ?, ?, CAST(? AS user_type));"""
            );
            int parameterIndex = 0;
            st.setString(++parameterIndex, id);
            st.setString(++parameterIndex, authProvider);
            st.setString(++parameterIndex, providerSubjectId);
            st.setString(++parameterIndex, accessTypeForNewUser().toString());
            st.setString(++parameterIndex, subjectType.name());
            st.executeUpdate();

            return switch (subjectType) {
                case USER -> new User(id);
                case SERVANT -> new Servant(id);
            };
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    public Subject subject(String id) throws AuthException {
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement("""
                SELECT user_id FROM users
                WHERE user_id = ?;"""
            );
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

    public void removeSubject(Subject subject) throws AuthException {
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement(
                "DELETE FROM users WHERE user_id = ?;"
            );
            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement("""
                INSERT INTO credentials (
                name,
                "value",
                user_id,
                type
                )
                VALUES (?, ?, ?, ?);"""
            );
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

    public SubjectCredentials credentials(Subject subject, String name) throws AuthException {
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement("""
                SELECT name, "value", type FROM credentials
                WHERE user_id = ?
                AND name = ?;"""
            );
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

    public void removeCredentials(Subject subject, String name) throws AuthException {
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement(
                "DELETE FROM credentials WHERE user_id = ? AND name = ?;"
            );
            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.setString(++parameterIndex, name);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    private UserVerificationType accessTypeForNewUser() {
        if (serviceConfig.getUserLimit() == 0) {
            return UserVerificationType.ACCESS_ALLOWED;
        }
        try (var connect = storage.connect()) {
            final PreparedStatement st = connect.prepareStatement(
                    "SELECT count(*) from users where access_type = ?;"
            );
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
