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
import com.google.common.annotations.VisibleForTesting;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbSubjectService {
    private static final Logger LOG = LogManager.getLogger(DbSubjectService.class);

    public static final String QUERY_INSERT_SUBJECT_IF_NOT_EXISTS_AND_RETURN_STORED = """
        WITH
            row_to_insert (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
            AS (VALUES (?, ?, ?, ?, ?, ?)),
            attempt_to_insert AS
            (
                INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
                SELECT * FROM row_to_insert
                ON CONFLICT (auth_provider, provider_user_id) DO NOTHING
                RETURNING user_id, auth_provider, provider_user_id, access_type, user_type, request_hash
            )
        SELECT 
            COALESCE(attempt_to_insert.user_id, users.user_id) AS user_id,
            COALESCE(attempt_to_insert.request_hash, users.request_hash) AS request_hash
        FROM row_to_insert
        LEFT JOIN users
        ON users.auth_provider = row_to_insert.auth_provider
            AND users.provider_user_id = row_to_insert.provider_user_id
        LEFT JOIN attempt_to_insert
        ON attempt_to_insert.auth_provider = row_to_insert.auth_provider
            AND attempt_to_insert.provider_user_id = row_to_insert.provider_user_id""";

    private static final String QUERY_INSERT_CREDENTIALS = """
        INSERT INTO credentials (name, value, user_id, type, expired_at)
        VALUES (?, ?, ?, ?, ?)""";

    private static final String QUERY_INSERT_CREDENTIALS_IF_NOT_EXISTS_AND_RETURN_STORED = """
        WITH
            row_to_insert (name, value, user_id, type, expired_at) AS (VALUES (?, ?, ?, ?, ?)),
            attempt_to_insert AS
            (
                INSERT INTO credentials (name, value, user_id, type, expired_at)
                SELECT name, value, user_id, type, CAST(expired_at as timestamp without time zone) FROM row_to_insert
                ON CONFLICT (name, user_id) DO NOTHING
                RETURNING name, value, user_id, type, expired_at
            )
        SELECT
            COALESCE(attempt_to_insert.value, credentials.value) AS value,
            COALESCE(attempt_to_insert.type, credentials.type) AS type,
            COALESCE(attempt_to_insert.expired_at, credentials.expired_at) AS expired_at
        FROM row_to_insert
        LEFT JOIN credentials
        ON credentials.name = row_to_insert.name AND credentials.user_id = row_to_insert.user_id
        LEFT JOIN attempt_to_insert
        ON attempt_to_insert.name = row_to_insert.name AND attempt_to_insert.user_id = row_to_insert.user_id""";

    @Inject
    private IamDataSource storage;

    @Inject
    private ServiceConfig serviceConfig;

    public Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType subjectType,
                                 List<SubjectCredentials> credentials, String requestHash) throws AuthException
    {
        LOG.debug("Create subject {}/{}/{} with credentials [{}]", authProvider, providerSubjectId, subjectType,
            credentials.stream().map(Record::toString).collect(Collectors.joining(", ")));

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
                         var st = connect.prepareStatement(QUERY_INSERT_SUBJECT_IF_NOT_EXISTS_AND_RETURN_STORED))
                    {
                        int parameterIndex = 0;
                        st.setString(++parameterIndex, subjectId);
                        st.setString(++parameterIndex, authProvider.name());
                        st.setString(++parameterIndex, providerSubjectId);
                        st.setString(++parameterIndex, accessTypeForNewUser(connect).toString());
                        st.setString(++parameterIndex, subjectType.name());
                        st.setString(++parameterIndex, requestHash);

                        ResultSet rs = st.executeQuery();

                        rs.next();
                        var actualSubjectId = rs.getString("user_id");
                        var actualRequestHash = rs.getString("request_hash");

                        if (!requestHash.contentEquals(actualRequestHash)) {
                            throw new PSQLException(String.format("Subject with auth_provider '%s' and " +
                                "provider_user_id '%s' already exists", authProvider.name(), providerSubjectId),
                                PSQLState.UNIQUE_VIOLATION);
                        }

                        return switch (subjectType) {
                            case USER -> new User(actualSubjectId);
                            case SERVANT -> new Servant(actualSubjectId);
                            case VM -> new Vm(actualSubjectId);
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
                     var connect = tx.connect();
                     var subjSt = connect.prepareStatement(QUERY_INSERT_SUBJECT_IF_NOT_EXISTS_AND_RETURN_STORED);
                     var credsSt = connect.prepareStatement(QUERY_INSERT_CREDENTIALS))
                {
                    int parameterIndex = 0;
                    subjSt.setString(++parameterIndex, subjectId);
                    subjSt.setString(++parameterIndex, authProvider.name());
                    subjSt.setString(++parameterIndex, providerSubjectId);
                    subjSt.setString(++parameterIndex, accessTypeForNewUser(connect).toString());
                    subjSt.setString(++parameterIndex, subjectType.name());
                    subjSt.setString(++parameterIndex, requestHash);

                    ResultSet rs = subjSt.executeQuery();

                    rs.next();
                    var actualSubjectId = rs.getString("user_id");
                    var actualRequestHash = rs.getString("request_hash");

                    if (!requestHash.contentEquals(actualRequestHash)) {
                        throw new PSQLException(String.format("Subject with auth_provider '%s' and " +
                            "provider_user_id '%s' already exists", authProvider.name(), providerSubjectId),
                            PSQLState.UNIQUE_VIOLATION);
                    }

                    if (subjectId.contentEquals(actualSubjectId)) {
                        for (var creds : credentials) {
                            credsSt.clearParameters();
                            credsSt.setString(1, creds.name());
                            credsSt.setString(2, creds.value());
                            credsSt.setString(3, subjectId);
                            credsSt.setString(4, creds.type().name());

                            var expiredAtTimestamp = (creds.expiredAt() != null)
                                ? Timestamp.from(creds.expiredAt().truncatedTo(ChronoUnit.SECONDS))
                                : null;

                            if (creds.expiredAt() != null) {
                                credsSt.setTimestamp(5, expiredAtTimestamp);
                            } else {
                                credsSt.setNull(5, Types.TIMESTAMP);
                            }

                            credsSt.addBatch();
                        }
                        credsSt.executeBatch();
                    }

                    tx.commit();

                    return switch (subjectType) {
                        case USER -> new User(actualSubjectId);
                        case SERVANT -> new Servant(actualSubjectId);
                        case VM -> new Vm(actualSubjectId);
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
                            case VM -> new Vm(id);
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

    public void addCredentials(Subject subject, SubjectCredentials credentials) throws AuthException {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var connect = storage.connect();
                     var st = connect.prepareStatement(QUERY_INSERT_CREDENTIALS_IF_NOT_EXISTS_AND_RETURN_STORED))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, credentials.name());
                    st.setString(++parameterIndex, credentials.value());
                    st.setString(++parameterIndex, subject.id());
                    st.setString(++parameterIndex, credentials.type().name());

                    var expiredAtTimestamp = (credentials.expiredAt() != null)
                        ? Timestamp.from(credentials.expiredAt().truncatedTo(ChronoUnit.SECONDS))
                        : null;

                    if (credentials.expiredAt() != null) {
                        st.setTimestamp(++parameterIndex, expiredAtTimestamp);
                    } else {
                        st.setNull(++parameterIndex, Types.TIMESTAMP);
                    }

                    ResultSet rs = st.executeQuery();

                    if (rs.next()) {
                        var actualValue = rs.getString("value");
                        var actualType = CredentialsType.valueOf(rs.getString("type"));
                        var actualExpiredAt = rs.getTimestamp("expired_at");

                        if (!credentials.value().contentEquals(actualValue) || credentials.type() != actualType
                            || !Objects.equals(expiredAtTimestamp, actualExpiredAt))
                        {
                            throw new IllegalArgumentException(String.format("Credentials name '%s' is already used " +
                                "for another user '%s' credentials", credentials.name(), subject.id()));
                        }
                    }
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
                         SELECT name, value, type, expired_at
                         FROM credentials
                         WHERE user_id = ? AND name = ? AND (expired_at IS NULL OR expired_at > NOW())"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, subject.id());
                    st.setString(++parameterIndex, name);
                    ResultSet rs = st.executeQuery();
                    if (rs.next()) {
                        var expiredAt = rs.getTimestamp("expired_at");
                        return new SubjectCredentials(
                            rs.getString("name"),
                            rs.getString("value"),
                            CredentialsType.valueOf(rs.getString("type")),
                            expiredAt != null ? expiredAt.toInstant() : null);
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
                         SELECT name, value, type, expired_at
                         FROM credentials
                         WHERE user_id = ? AND (expired_at IS NULL OR expired_at > NOW())"""))
                {
                    int parameterIndex = 0;
                    st.setString(++parameterIndex, subject.id());
                    ResultSet rs = st.executeQuery();
                    List<SubjectCredentials> result = new ArrayList<>();
                    while (rs.next()) {
                        var expiredAt = rs.getTimestamp("expired_at");
                        result.add(
                            new SubjectCredentials(
                                rs.getString("name"),
                                rs.getString("value"),
                                CredentialsType.valueOf(rs.getString("type")),
                                expiredAt != null ? expiredAt.toInstant() : null));
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

    @VisibleForTesting
    @Nullable
    public Subject getSubjectForTests(AuthProvider authProvider, String providerSubjectId, SubjectType subjectType)
        throws SQLException
    {
        LOG.debug("Looking for subject {}/{}/{}...", authProvider, providerSubjectId, subjectType);

        try (var connect = storage.connect();
             var st = connect.prepareStatement("""
                 SELECT user_id
                 FROM users
                 WHERE auth_provider = ? AND provider_user_id = ? AND user_type = ?"""))
        {
            st.setString(1, authProvider.name());
            st.setString(2, providerSubjectId);
            st.setString(3, subjectType.name());

            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                var id = rs.getString("user_id");
                return switch (subjectType) {
                    case USER -> new User(id);
                    case SERVANT -> new Servant(id);
                    case VM -> new Vm(id);
                };
            }

            return null;
        }
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
