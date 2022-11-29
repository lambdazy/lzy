package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.storage.LzyServiceStorage;
import jakarta.inject.Singleton;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import javax.annotation.Nullable;

@Singleton
public class GcDaoImpl implements GcDao {
    private static final String QUERY_UPDATE_GC = """
        UPDATE garbage_collectors
        SET updated_at = ?
        WHERE gc_instance_id = ?""";

    private static final String QUERY_INSERT_GC = """
        INSERT INTO garbage_collectors (gc_instance_id, updated_at, status)
        VALUES (?, ?, ?)""";

    private static final String QUERY_GET_UPDATED_AT = """
        SELECT MAX(updated_at)
        FROM garbage_collectors""";

    private final Storage storage;

    public GcDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void insertNewGcSession(@Nullable TransactionHandle transaction, String id) throws SQLException {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_INSERT_GC)) {
                statement.setString(1, id);
                statement.setTimestamp(2, Timestamp.from(Instant.now()));
                statement.setString(3, "");
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateStatus(@Nullable TransactionHandle transaction, String id) throws SQLException {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_GC)) {
                statement.setTimestamp(1, Timestamp.from(Instant.now()));
                statement.setString(2, id);
                statement.executeUpdate();
            }
        });
    }

    @Nullable
    @Override
    public Timestamp getLastUpdated() throws SQLException {
        Timestamp[] result = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_UPDATED_AT)) {
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    result[0] = rs.getTimestamp(1);
                }
            }
        });

        return result[0];
    }
}
