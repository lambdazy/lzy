package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.Storage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.*;

@Singleton
@Replaces(Storage.class)
public class MockDbStorage implements DbStorage {
    private final SessionFactory sessionFactory;

    public MockDbStorage(){
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        cfg.setProperty("hibernate.connection.driver_class","org.h2.Driver");
        cfg.setProperty("hibernate.dialect",
                "org.hibernate.dialect.H2Dialect");
        cfg.setProperty("hibernate.hbm2ddl.auto", "create");
        cfg.addAnnotatedClass(EntryDependenciesModel.class);
        cfg.addAnnotatedClass(WhiteboardFieldModel.class);
        cfg.addAnnotatedClass(SnapshotOwnerModel.class);
        cfg.addAnnotatedClass(SnapshotEntryModel.class);
        cfg.addAnnotatedClass(WhiteboardModel.class);
        cfg.addAnnotatedClass(SnapshotModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    @Override
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}