package ai.lzy.allocator.db;

import ai.lzy.model.db.Storage;
import jakarta.inject.Singleton;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class TransactionManagerImpl implements TransactionManager {
    private final Storage storage;

    public TransactionManagerImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public TransactionHandle start() {
        return new TransactionHandleImpl(storage);
    }

    public static class TransactionHandleImpl implements TransactionHandle {
        private final Storage storage;
        private final AtomicBoolean committed = new AtomicBoolean(false);

        private Connection con = null;

        public TransactionHandleImpl(Storage storage) {
            this.storage = storage;
        }

        @Override
        public Connection connect() throws SQLException {
            if (con != null) {
                return con;
            }
            con = storage.connect();
            con.setAutoCommit(false);
            return con;
        }

        public void commit() throws SQLException {
            if (con == null) {
                return;
            }
            con.commit();
            committed.set(true);
        }


        @Override
        public void close() throws SQLException {
            if (con == null) {
                return;
            }
            if (!committed.get()) {
                con.rollback();
            }
            con.setAutoCommit(true);
            con.close();
        }
    }
}
