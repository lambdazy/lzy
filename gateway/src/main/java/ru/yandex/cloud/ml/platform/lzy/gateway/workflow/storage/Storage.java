package ru.yandex.cloud.ml.platform.lzy.gateway.workflow.storage;

import java.sql.Connection;
import java.sql.SQLException;

public interface Storage {
    Connection connect() throws SQLException;
}
