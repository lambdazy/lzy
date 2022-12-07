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
import jakarta.inject.Singleton;
import lombok.Lombok;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import javax.annotation.Nullable;

@Singleton
public class SessionDaoImpl implements SessionDao {
    private static final Logger LOG = LogManager.getLogger(SessionDaoImpl.class);

    private final Storage storage;
    private final ObjectMapper objectMapper;
    private volatile Throwable injectedError = null;

    public SessionDaoImpl(AllocatorDataSource storage, ObjectMapper objectMapper) {
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

        final Session[] session = {null};
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                SELECT id, owner, description, cache_policy_json, op_id
                FROM session
                WHERE id = ? AND deleted_at IS NULL""" + forUpdate(transaction)))
            {
                s.setString(1, sessionId);
                final var rs = s.executeQuery();
                if (!rs.next()) {
                    session[0] = null;
                    return;
                }
                final var id = rs.getString(1);
                final var owner = rs.getString(2);
                final var description = rs.getString(3);
                final var cachePolicy = objectMapper.readValue(rs.getString(4), CachePolicy.class);
                final var opId = rs.getString(5);
                session[0] = new Session(id, owner, description, cachePolicy, opId);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot parse cache policy for session " + sessionId, e);
            }
        });
        return session[0];
    }

    @Override
    public boolean delete(String sessionId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Delete session {} in tx {}", sessionId, transaction);

        throwInjectedError();

        final boolean[] ret = {false};

        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                UPDATE session
                SET deleted_at = now()
                WHERE id = ? AND deleted_at IS NULL"""))
            {
                s.setString(1, sessionId);
                ret[0] = s.executeUpdate() > 0;
            }
        });

        return ret[0];
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
