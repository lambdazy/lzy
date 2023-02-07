package ai.lzy.allocator.alloc.dao.impl;

import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
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
import java.util.ArrayList;
import java.util.List;

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
            try (PreparedStatement s = con.prepareStatement("""
                INSERT INTO session(id, owner, description, cache_policy_json, create_op_id)
                VALUES (?, ?, ?, ?, ?)"""))
            {
                s.setString(1, session.sessionId());
                s.setString(2, session.owner());
                s.setString(3, session.description());
                s.setString(4, objectMapper.writeValueAsString(session.cachePolicy()));
                s.setString(5, session.allocateOpId());
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
                SELECT id, owner, description, cache_policy_json, create_op_id, delete_op_id
                FROM session
                WHERE id = ? AND delete_op_id IS NULL""" + forUpdate(transaction)))
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
    public Session delete(String sessionId, String deleteOpId, @Nullable TransactionHandle tx) throws SQLException {
        LOG.debug("Delete session {} in tx {}", sessionId, tx);

        throwInjectedError();

        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement st = con.prepareStatement("""
                UPDATE session
                SET modified_at = NOW(), delete_op_id = ?
                WHERE id = ?
                RETURNING id, owner, description, cache_policy_json, create_op_id, delete_op_id"""))
            {
                st.setString(1, deleteOpId);
                st.setString(2, sessionId);
                var rs = st.executeQuery();
                if (rs.next()) {
                    return readSession(rs);
                }
                return null;
            }
        });
    }

    @Override
    public void touch(String sessionId, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement("""
                UPDATE session
                SET modified_at = NOW()
                WHERE id = ? AND delete_op_id IS NULL"""))
            {
                st.setString(1, sessionId);
                var ret = st.executeUpdate();
                if (ret != 1) {
                    throw new NotFoundException("Session %s not found".formatted(sessionId));
                }
            }
        });
    }

    @Override
    public List<Session> listDeleting(@Nullable TransactionHandle transaction) throws SQLException {
        return DbOperation.execute(transaction, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement("""
                SELECT id, owner, description, cache_policy_json, create_op_id, delete_op_id
                FROM session
                WHERE delete_op_id IS NOT NULL"""))
            {
                var rs = st.executeQuery();
                var sessions = new ArrayList<Session>();
                while (rs.next()) {
                    sessions.add(readSession(rs));
                }
                return sessions;
            }
        });
    }

    // TODO: full scan here
    @Override
    public int countActiveSessions() throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement("SELECT COUNT(*) FROM session WHERE delete_op_id IS NULL"))
        {
            var rs = st.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
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
        final var allocOpId = rs.getString(5);
        final var deleteOpId = rs.getString(6);
        return new Session(id, owner, description, cachePolicy, allocOpId, deleteOpId);
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
