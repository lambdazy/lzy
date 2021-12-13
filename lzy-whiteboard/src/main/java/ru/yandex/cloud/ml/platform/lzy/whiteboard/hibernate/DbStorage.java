package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import org.hibernate.SessionFactory;

public interface DbStorage {
    SessionFactory getSessionFactory();
}
