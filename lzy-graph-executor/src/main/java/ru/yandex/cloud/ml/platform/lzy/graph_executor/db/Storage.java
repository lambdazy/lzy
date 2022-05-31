package ru.yandex.cloud.ml.platform.lzy.graph_executor.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface Storage {
    Connection connect() throws SQLException;
}
