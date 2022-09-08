package ai.lzy.iam.storage.impl;

import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.*;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.UserVerificationType;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
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

    public Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType subjectType,
                                 List<SubjectCredentials> credentials)
        throws AuthException
    {
        if (authProvider.isInternal() && subjectType == SubjectType.USER) {
            throw new AuthInternalException("Invalid auth provider");
        }

        final var subjectId = UUID.randomUUID().toString();

        if (credentials.isEmpty()) {
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
                        st.setString(++parameterIndex, subjectId);
                        st.setString(++parameterIndex, authProvider.name());
                        st.setString(++parameterIndex, providerSubjectId);
                        st.setString(++parameterIndex, accessTypeForNewUser(connect).toString());
                        st.setString(++parameterIndex, subjectType.name());
                        st.executeUpdate();

                        return switch (subjectType) {
                            case USER -> new User(subjectId);
                            case SERVANT -> new Servant(subjectId);
                        };
                    }
                },
                AuthInternalException::new);
        }


        return withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var tx = TransactionHandle.create(storage);
                     var conn = tx.connect();
                     var subjSt = conn.prepareStatement("""
                            INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type)
                            VALUES (?, ?, ?, ?, ?)""");
                     var credsSt = conn.prepareStatement("""
                            INSERT INTO credentials (name, value, user_id, type)
                            VALUES (?, ?, ?, ?)"""))
                {
                    subjSt.setString(1, subjectId);
                    subjSt.setString(2, authProvider.name());
                    subjSt.setString(3, providerSubjectId);
                    subjSt.setString(4, accessTypeForNewUser(conn).toString());
                    subjSt.setString(5, subjectType.name());
                    subjSt.executeUpdate();

                    for (var creds : credentials) {
                        credsSt.clearParameters();
                        credsSt.setString(1, creds.name());
                        credsSt.setString(2, creds.value());
                        credsSt.setString(3, subjectId);
                        credsSt.setString(4, creds.type().name());
                        credsSt.addBatch();
                    }
                    credsSt.executeBatch();

                    tx.commit();

                    return switch (subjectType) {
                        case USER -> new User(subjectId);
                        case SERVANT -> new Servant(subjectId);
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
                         "SELECT user_type FROM users WHERE user_id = ?"))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, id);
                    ResultSet rs = st.executeQuery();
                    if (rs.next()) {
                        final SubjectType type = SubjectType.valueOf(rs.getString("user_type"));
                        return switch (type) {
                            case USER -> new User(id);
                            case SERVANT -> new Servant(id);
                        };
                    }

                    throw new AuthNotFoundException("Subject:: " + id + " NOT_FOND");
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
            },
            AuthInternalException::new);
    }

    public void addCredentials(Subject subject, String name, String value, CredentialsType type) throws AuthException {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement("""
                        INSERT INTO credentials (name, value, user_id, type)
                        VALUES (?, ?, ?, ?)"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, name);
                    st.setString(++parameterIndex, value);
                    st.setString(++parameterIndex, subject.id());
                    st.setString(++parameterIndex, type.name());
                    st.executeUpdate();
                }
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
                        SELECT name, value, type
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
                            CredentialsType.valueOf(rs.getString("type")));
                    }

                    throw new AuthNotFoundException("Credentials:: " + name + " NOT_FOND");
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
                        SELECT name, value, type
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
                                CredentialsType.valueOf(rs.getString("type"))));
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
