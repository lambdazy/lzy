package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import org.hibernate.SessionFactory;

public interface DbStorage {
    SessionFactory getSessionFactory();
}
