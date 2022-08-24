package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface TransactionHandle extends AutoCloseable {

    Connection connect() throws SQLException;

    void commit() throws SQLException;

    @Override
    void close() throws SQLException;

}
