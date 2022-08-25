package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;

public final class TransactionHandleImpl implements TransactionHandle {

    private final Storage storage;
    private boolean committed = false;
    private Connection con = null;

    public TransactionHandleImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public synchronized Connection connect() throws SQLException {
        if (con != null) {
            return con;
        }
        con = storage.connect();
        con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        con.setAutoCommit(false);
        return con;
    }

    @Override
    public synchronized void commit() throws SQLException {
        if (con == null) {
            return;
        }
        if (committed) {
            throw new RuntimeException("Already committed");
        }
        con.commit();
        committed = true;
    }

    @Override
    public synchronized void close() throws SQLException {
        if (con == null) {
            return;
        }
        if (!committed) {
            con.rollback();
        }
        con.setAutoCommit(true);
        con.close();
        con = null;
    }
}
