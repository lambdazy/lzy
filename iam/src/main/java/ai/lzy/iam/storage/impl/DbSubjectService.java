package ai.lzy.iam.storage.impl;

import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Servant;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.UserVerificationType;
import ai.lzy.util.auth.exceptions.AuthBadRequestException;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbSubjectService {
    private static final Logger LOG = LogManager.getLogger(DbSubjectService.class);

    @Inject
    private IamDataSource storage;

    @Inject
    private ServiceConfig serviceConfig;

    public Subject createSubject(String authProvider, String providerSubjectId, SubjectType subjectType)
        throws AuthException
    {
        final var id = UUID.randomUUID().toString();

        return withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement("""
                        INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type)
                        VALUES (?, ?, ?, ?, ?)"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, id);
                    st.setString(++parameterIndex, authProvider);
                    st.setString(++parameterIndex, providerSubjectId);
                    st.setString(++parameterIndex, accessTypeForNewUser(connect).toString());
                    st.setString(++parameterIndex, subjectType.name());
                    st.executeUpdate();

                    return switch (subjectType) {
                        case USER -> new User(id);
                        case SERVANT -> new Servant(id);
                    };
                }
            },
            AuthInternalException::new);
    }

    public Subject subject(String id) throws AuthException {
        return withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement(
                         "SELECT user_id, user_type FROM users WHERE user_id = ?"))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, id);
                    ResultSet rs = st.executeQuery();
                    if (rs.next()) {
                        final String user_id = rs.getString("user_id");
                        final SubjectType type = SubjectType.valueOf(rs.getString("user_type"));
                        return switch (type) {
                            case USER -> new User(user_id);
                            case SERVANT -> new Servant(user_id);
                        };
                    }

                    throw new AuthBadRequestException("Subject:: " + id + " NOT_FOND");
                }
            },
            DbSubjectService::wrapError);
    }

    public void removeSubject(Subject subject) throws AuthException {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement(
                         "DELETE FROM users WHERE user_id = ?"))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, subject.id());
                    st.executeUpdate();
                }
                return (Void) null;
            },
            AuthInternalException::new);
    }

    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement("""
                        INSERT INTO credentials (name, "value", user_id, type)
                        VALUES (?, ?, ?, ?)"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, name);
                    st.setString(++parameterIndex, value);
                    st.setString(++parameterIndex, subject.id());
                    st.setString(++parameterIndex, type);
                    st.executeUpdate();
                }

                return (Void) null;
            },
            AuthInternalException::new);
    }

    public SubjectCredentials credentials(Subject subject, String name) throws AuthException {
        return withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement("""
                        SELECT name, "value", type
                        FROM credentials
                        WHERE user_id = ? AND name = ?"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, subject.id());
                    st.setString(++parameterIndex, name);
                    ResultSet rs = st.executeQuery();
                    if (rs.next()) {
                        return new SubjectCredentials(
                            rs.getString("name"),
                            rs.getString("value"),
                            rs.getString("type"));
                    }

                    throw new AuthBadRequestException("Credentials:: " + name + " NOT_FOND");
                }
            },
            DbSubjectService::wrapError);
    }

    public List<SubjectCredentials> listCredentials(Subject subject) throws AuthException {
        return withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement("""
                        SELECT *
                        FROM credentials
                        WHERE user_id = ?"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, subject.id());
                    ResultSet rs = st.executeQuery();
                    List<SubjectCredentials> result = new ArrayList<>();
                    while (rs.next()) {
                        result.add(
                            new SubjectCredentials(
                                rs.getString("name"),
                                rs.getString("value"),
                                rs.getString("type")));
                    }
                    return result;
                }
            },
            AuthInternalException::new);
    }

    public void removeCredentials(Subject subject, String name) throws AuthException {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement(
                         "DELETE FROM credentials WHERE user_id = ? AND name = ?"))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, subject.id());
                    st.setString(++parameterIndex, name);
                    st.executeUpdate();
                }
                return (Void) null;
            },
            AuthInternalException::new);
    }

    private UserVerificationType accessTypeForNewUser(Connection connect) {
        if (serviceConfig.getUserLimit() == 0) {
            return UserVerificationType.ACCESS_ALLOWED;
        }

        return withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var st = connect.prepareStatement(
                    "SELECT count(*) from users where access_type = ?"))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, UserVerificationType.ACCESS_ALLOWED.toString());
                    final ResultSet rs = st.executeQuery();
                    if (rs.next()) {
                        if (rs.getInt(1) < serviceConfig.getUserLimit()) {
                            return UserVerificationType.ACCESS_ALLOWED;
                        } else {
                            return UserVerificationType.ACCESS_PENDING;
                        }
                    }

                    throw new AuthInternalException("Unknown active users count");
                }
            },
            DbSubjectService::wrapError);
    }

    private static AuthException wrapError(Exception ex) {
        if (ex instanceof AuthException e) {
            return e;
        } else {
            return new AuthInternalException(ex);
        }
    }
}
