package ai.lzy.model.db;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;

public interface DbOperation {
    void execute(Connection connection) throws SQLException;

    static void execute(@Nullable TransactionHandle transaction, Storage storage, DbOperation op) {
        final Connection con;
        try {
            if (transaction == null) {
                con = storage.connect();
            } else {
                con = transaction.connect();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot connect to db", e);
        }

        try {
            op.execute(con);
        } catch (SQLException e) {
            throw new RuntimeException("Cannot execute sql request", e);
        }
        try {
            if (transaction == null) {
                con.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot close connection", e);
        }
    }
}
