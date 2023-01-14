package ai.lzy.allocator.alloc.dao.impl;

import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.Lombok;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
public class SessionDaoImpl implements SessionDao {
    private static final Logger LOG = LogManager.getLogger(SessionDaoImpl.class);

    private final Storage storage;
    private final ObjectMapper objectMapper;
    private volatile Throwable injectedError = null;

    public SessionDaoImpl(AllocatorDataSource storage, @Named("AllocatorObjectMapper") ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void create(Session session, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Create session {}", session);

        throwInjectedError();

        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                INSERT INTO session(id, owner, description, cache_policy_json, op_id)
                VALUES (?, ?, ?, ?, ?)"""))
            {
                s.setString(1, session.sessionId());
                s.setString(2, session.owner());
                s.setString(3, session.description());
                s.setString(4, objectMapper.writeValueAsString(session.cachePolicy()));
                s.setString(5, session.opId());
                s.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump cache policy %s".formatted(session.cachePolicy()), e);
            }
        });
    }

    @Nullable
    @Override
    public Session get(String sessionId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Get session {} in tx {}", sessionId, transaction);

        throwInjectedError();

        return DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement("""
                SELECT id, owner, description, cache_policy_json, op_id
                FROM session
                WHERE id = ?""" + forUpdate(transaction)))
            {
                s.setString(1, sessionId);
                final var rs = s.executeQuery();
                if (rs.next()) {
                    return readSession(rs);
                }
                return null;
            }
        });
    }

    @Override
    @Nullable
    public Session delete(String sessionId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Delete session {} in tx {}", sessionId, transaction);

        throwInjectedError();

        return DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement st = con.prepareStatement("""
                DELETE FROM session
                WHERE id = ?
                RETURNING id, owner, description, cache_policy_json, op_id"""))
            {
                st.setString(1, sessionId);
                var rs = st.executeQuery();
                if (rs.next()) {
                    return readSession(rs);
                }
                return null;
            }
        });
    }

    private Session readSession(ResultSet rs) throws SQLException {
        final var id = rs.getString(1);
        final var owner = rs.getString(2);
        final var description = rs.getString(3);
        final CachePolicy cachePolicy;
        try {
            cachePolicy = objectMapper.readValue(rs.getString(4), CachePolicy.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot parse cache policy for session " + id, e);
        }
        final var opId = rs.getString(5);
        return new Session(id, owner, description, cachePolicy, opId);
    }

    @VisibleForTesting
    public void injectError(Throwable error) {
        injectedError = error;
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored", "ThrowableNotThrown"})
    private void throwInjectedError() {
        final var error = injectedError;
        if (error != null) {
            injectedError = null;
            Lombok.sneakyThrow(error);
        }
    }

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
