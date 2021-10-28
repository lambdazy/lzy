package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TaskModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;

@Singleton
public class Storage implements DbStorage{
    private final SessionFactory sessionFactory;
    @Inject
    DbConfig config;

    public Storage(){
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", config.getUrl());
        cfg.setProperty("hibernate.connection.username", config.getUsername());
        cfg.setProperty("hibernate.connection.password", config.getPassword());
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.addAnnotatedClass(UserModel.class);
        cfg.addAnnotatedClass(TaskModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
