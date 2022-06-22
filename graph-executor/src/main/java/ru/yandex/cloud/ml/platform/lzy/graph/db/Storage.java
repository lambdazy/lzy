package ru.yandex.cloud.ml.platform.lzy.graph.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface Storage {
    Connection connect() throws SQLException;
}
