package ru.yandex.cloud.ml.platform.lzy.graph.db;

public class DaoException extends Exception {
    public DaoException(Exception e) {
        super(e);
    }

    public DaoException(String e) {
        super(e);
    }
}
