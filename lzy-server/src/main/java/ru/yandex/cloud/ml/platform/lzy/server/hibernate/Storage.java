package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class Storage {
    private final SessionFactory sessionFactory;

    public Storage(DbConfig config){
        Configuration cfg = new Configuration().configure();
        cfg.setProperty("hibernate.connection.url", config.getUrl());
        cfg.setProperty("hibernate.connection.username", config.getUsername());
        cfg.setProperty("hibernate.connection.password", config.getPassword());
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.addAnnotatedClass(UserModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
