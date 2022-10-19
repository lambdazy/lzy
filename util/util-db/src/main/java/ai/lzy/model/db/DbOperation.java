package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nullable;

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
}
