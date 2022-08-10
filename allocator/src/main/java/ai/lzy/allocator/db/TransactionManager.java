package ai.lzy.allocator.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface TransactionManager {

    TransactionHandle start();


    interface TransactionHandle extends AutoCloseable {
        void commit() throws SQLException;
        void close() throws SQLException;
        Connection connect() throws SQLException;
    }

}
