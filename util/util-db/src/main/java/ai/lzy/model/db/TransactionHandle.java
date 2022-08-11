package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionHandle implements AutoCloseable {
    private final Storage storage;
    private final AtomicBoolean committed = new AtomicBoolean(false);

    private Connection con = null;

    public TransactionHandle(Storage storage) {
        this.storage = storage;
    }

    public synchronized Connection connect() throws SQLException {
        if (con != null) {
            return con;
        }
        con = storage.connect();
        con.setAutoCommit(false);
        return con;
    }

    public synchronized void commit() throws SQLException {
        if (con == null) {
            return;
        }
        if (committed.get()) {
            throw new RuntimeException("Already committed");
        }
        con.commit();
        committed.set(true);
    }

    @Override
    public synchronized void close() throws SQLException {
        if (con == null) {
            return;
        }
        if (!committed.get()) {
            con.rollback();
        }
        con.setAutoCommit(true);
        con.close();
        con = null;
    }
}
