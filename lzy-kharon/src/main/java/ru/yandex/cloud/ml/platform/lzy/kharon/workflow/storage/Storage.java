package ru.yandex.cloud.ml.platform.lzy.kharon.workflow.storage;

import java.sql.Connection;
import java.sql.SQLException;

public interface Storage {

    /**
     * @return connection with `auto commit` flag on
     */
    Connection connect() throws SQLException;

}
