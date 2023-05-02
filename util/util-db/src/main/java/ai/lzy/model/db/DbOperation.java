package ai.lzy.model.db;

import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

public interface DbOperation {
    static void execute(@Nullable TransactionHandle transaction, Storage storage, DbRunnable op)
        throws SQLException
    {
        execute(transaction, storage, (con) -> {
            op.execute(con);
            return null;
        });
    }

    static <T> T execute(@Nullable TransactionHandle transaction, Storage storage, DbSupplier<T> op)
        throws SQLException
    {
        var con = transaction == null ? storage.connect() : transaction.connect();
        try {
            return op.execute(con);
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

    interface DbRunnable {
        void execute(Connection connection) throws SQLException;
    }

    interface DbSupplier<T> {
        T execute(Connection connection) throws SQLException;
    }

}
