package ai.lzy.allocator.gc.dao;

import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

@Singleton
public class GcDao {
    private static final Logger LOG = LogManager.getLogger(GcDao.class);

    private static final String QUERY_GET_LEADER = """
        SELECT owner, expired_at
        FROM gc_lease
        WHERE gc = 'default' AND expired_at > NOW()
        """;

    private static final String QUERY_PROLONG_LEASE = """
        UPDATE gc_lease
        SET updated_at = NOW(),
            expired_at = NOW() + CAST(? AS INTERVAL)
        WHERE gc = 'default' AND owner = ?
        RETURNING expired_at""";

    private static final String QUERY_ACQUIRE = """
        UPDATE gc_lease AS new
        SET owner = ?,
            updated_at = NOW(),
            expired_at = NOW() + CAST(? AS INTERVAL)
        FROM gc_lease AS old
        WHERE old.gc = new.gc
          AND old.gc = 'default'
          AND (old.expired_at < NOW() OR old.owner = ?)
        RETURNING old.owner AS prev_owner, old.expired_at AS prev_expired_at,
                  new.owner AS new_owner, new.expired_at AS new_expired_at""";

    private static final String QUERY_RELEASE = """
        UPDATE gc_lease
        SET owner = 'none', updated_at = NOW(), expired_at = NOW()
        WHERE gc = 'default' AND owner = ?""";


    private final AllocatorDataSource storage;

    @Inject
    public GcDao(AllocatorDataSource storage) {
        this.storage = storage;
    }

    public record Leader(String owner, Instant expiredAt) {}

    @Nullable
    public Leader getLeader() throws SQLException {
        return DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_GET_LEADER)) {
                var rs = st.executeQuery();

                if (rs.next()) {
                    return new Leader(
                        rs.getString("owner"),
                        rs.getTimestamp("expired_at").toInstant()
                    );
                } else {
                    return null;
                }
            }
        });
    }

    @Nullable
    public Instant prolongLeader(String owner, Duration duration) throws SQLException {
        return DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_PROLONG_LEASE)) {
                st.setString(1, "%s SECONDS".formatted(duration.getSeconds()));
                st.setString(2, owner);

                var rs = st.executeQuery();
                if (rs.next()) {
                    return rs.getTimestamp("expired_at").toInstant();
                }
                return null;
            }
        });
    }

    @Nullable
    public Instant tryAcquire(String owner, Duration duration, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_ACQUIRE)) {
                st.setString(1, owner);
                st.setString(2, "%s SECONDS".formatted(duration.getSeconds()));
                st.setString(3, owner);

                var rs = st.executeQuery();
                if (!rs.next()) {
                    return null;
                }

                var prevOwner = rs.getString("prev_owner");
                var until = rs.getTimestamp("new_expired_at");
                if (!owner.equals(prevOwner)) {
                    LOG.info("GC Leader was changed from {} (expired at {}) to {} (expires at {})",
                        prevOwner, rs.getTimestamp("prev_expired_at"), owner, until);
                }
                return until.toInstant();
            }
        });
    }

    public void release(String owner) throws SQLException {
        DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_RELEASE)) {
                st.setString(1, owner);
                st.execute();
            }
        });
    }
}
