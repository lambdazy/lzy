package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.*;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class Storage implements DbStorage{
    private final SessionFactory sessionFactory;

    @Inject
    public Storage(DbConfig config){
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", config.getUrl());
        cfg.setProperty("hibernate.connection.username", config.getUsername());
        cfg.setProperty("hibernate.connection.password", config.getPassword());
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.addAnnotatedClass(UserModel.class);
        cfg.addAnnotatedClass(TaskModel.class);
        cfg.addAnnotatedClass(TokenModel.class);
        cfg.addAnnotatedClass(UserRoleModel.class);
        cfg.addAnnotatedClass(PermissionModel.class);
        cfg.addAnnotatedClass(BackofficeSessionModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
