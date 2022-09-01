package ai.lzy.model.db;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;

public interface DbOperation {
    void execute(Connection connection) throws SQLException;

    static void execute(@Nullable TransactionHandle transaction, Storage storage, DbOperation op) throws SQLException {
        var con = transaction == null ? storage.connect() : transaction.connect();
        try {
            op.execute(con);
        } finally {
            if (transaction == null && con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    // ignored
                }
            }
        }
    }

    @Deprecated
    static void executeUnsafe(@Nullable TransactionHandle transaction, Storage storage, DbOperation op) {
        final Connection con;
        try {
            if (transaction == null) {
                con = storage.connect();
            } else {
                con = transaction.connect();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot connect to db: " + e.getMessage(), e);
        }

        try {
            op.execute(con);
        } catch (SQLException e) {
            if (transaction == null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new RuntimeException("Cannot execute sql request: " + ex.getMessage(), ex);
                }
            }
            throw new RuntimeException("Cannot execute sql request: " + e.getMessage(), e);
        }
        try {
            if (transaction == null) {
                con.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot close connection: " + e.getMessage(), e);
        }
    }
}
