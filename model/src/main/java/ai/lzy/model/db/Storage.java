package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface Storage {
    Connection connect() throws SQLException;
}
