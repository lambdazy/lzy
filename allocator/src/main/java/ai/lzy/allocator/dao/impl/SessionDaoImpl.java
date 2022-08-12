package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

@Singleton
public class SessionDaoImpl implements SessionDao {
    private final Storage storage;
    private final ObjectMapper objectMapper;

    public SessionDaoImpl(Storage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public Session create(String owner, CachePolicy cachePolicy, @Nullable TransactionHandle transaction) {
        final var session = new Session(UUID.randomUUID().toString(), owner, cachePolicy);
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                    "INSERT INTO session (id, owner, cache_policy_json) VALUES (?, ?, ?)")) {
                s.setString(1, session.sessionId());
                s.setString(2, session.owner());
                s.setString(3, objectMapper.writeValueAsString(session.cachePolicy()));
                s.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump cache policy", e);
            }
            if (transaction == null) {
                con.close();
            }
        });
        return session;
    }

    @Nullable
    @Override
    public Session get(String sessionId, @Nullable TransactionHandle transaction) {
        final Session[] session = new Session[1];
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                SELECT id, owner, cache_policy_json
                 FROM session
                 WHERE id = ?""")) {
                s.setString(1, sessionId);
                final var rs = s.executeQuery();
                if (!rs.next()) {
                    session[0] = null;
                    return;
                }
                final var id = rs.getString(1);
                final var owner = rs.getString(2);
                final var cachePolicy = objectMapper.readValue(rs.getString(3), CachePolicy.class);
                session[0] = new Session(id, owner, cachePolicy);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot parse cache policy", e);
            }
        });
        return session[0];
    }

    @Override
    public void delete(String sessionId, @Nullable TransactionHandle transaction) {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                DELETE FROM session
                 WHERE id = ?""")) {
                s.setString(1, sessionId);
                s.execute();
            }
        });
    }
}
