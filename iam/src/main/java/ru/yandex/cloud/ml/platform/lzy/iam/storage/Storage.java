package ru.yandex.cloud.ml.platform.lzy.iam.storage;

import java.sql.Connection;
import java.sql.SQLException;

public interface Storage {
    Connection connect() throws SQLException;
}
