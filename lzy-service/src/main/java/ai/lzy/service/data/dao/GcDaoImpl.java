package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.storage.LzyServiceStorage;
import jakarta.inject.Singleton;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.annotation.Nullable;

@Singleton
public class GcDaoImpl implements GcDao {

    private static final String QUERY_INSERT_GC = """
        INSERT INTO garbage_collectors (gc_instance_id, updated_at, valid_until)
        VALUES (?, ?, ?)""";

    private static final String QUERY_UPDATE_LEADER_GC = """
        SELECT gc_instance_id, updated_at, valid_until
        FROM garbage_collectors
        FOR UPDATE""";

    private static final String QUERY_UPDATE_VALID_GC = """
        SELECT updated_at, valid_until
        FROM garbage_collectors
        WHERE gc_instance_id = ?
        FOR UPDATE""";

    private final Storage storage;

    public GcDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void updateGC(String id, Timestamp now, Timestamp validUntil, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            var statement = con.prepareStatement(QUERY_UPDATE_LEADER_GC,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                if (rs.getTimestamp("valid_until").after(now)) {
                    throw new IllegalArgumentException("Already exists");
                }

                rs.updateTimestamp("updated_at", now);
                rs.updateTimestamp("valid_for", validUntil);
                rs.updateString("gc_instance_id", id);
                rs.updateRow();
            } else {
                try (var insertStatement = con.prepareStatement(QUERY_INSERT_GC)) {
                    insertStatement.setString(1, id);
                    insertStatement.setTimestamp(2, now);
                    insertStatement.setTimestamp(3, validUntil);
                    insertStatement.executeUpdate();
                }
            }
        });
    }

    @Override
    public void markGcValid(String id, Timestamp now, Timestamp validUntil, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            var statement = con.prepareStatement(QUERY_UPDATE_VALID_GC,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            statement.setString(1, id);

            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                if (rs.getTimestamp("valid_until").after(now)) {
                    throw new IllegalArgumentException("Already exists");
                }

                rs.updateTimestamp("updated_at", now);
                rs.updateTimestamp("valid_for", validUntil);
                rs.updateRow();
            } else {
                throw new IllegalStateException("Does not exist");
            }
        });
    }
}
