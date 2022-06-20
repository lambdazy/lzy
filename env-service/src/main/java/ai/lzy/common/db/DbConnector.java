package ai.lzy.common.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface DbConnector {
    Connection connect() throws SQLException;
}
