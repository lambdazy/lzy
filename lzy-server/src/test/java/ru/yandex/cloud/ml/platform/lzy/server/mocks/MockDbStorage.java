package ru.yandex.cloud.ml.platform.lzy.server.mocks;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.Storage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.BackofficeSessionModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.PermissionModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.PublicKeyModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TaskModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserRoleModel;

;

@Singleton
@Replaces(Storage.class)
@Requires(env = "test")
public class MockDbStorage implements DbStorage {
    private final SessionFactory sessionFactory;

    public MockDbStorage() {
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        cfg.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        cfg.setProperty("hibernate.dialect",
            "org.hibernate.dialect.H2Dialect");
        cfg.setProperty("hibernate.hbm2ddl.auto", "create");
        cfg.addAnnotatedClass(UserModel.class);
        cfg.addAnnotatedClass(TaskModel.class);
        cfg.addAnnotatedClass(UserRoleModel.class);
        cfg.addAnnotatedClass(PermissionModel.class);
        cfg.addAnnotatedClass(PublicKeyModel.class);
        cfg.addAnnotatedClass(UserRoleModel.class);
        cfg.addAnnotatedClass(PermissionModel.class);
        cfg.addAnnotatedClass(BackofficeSessionModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    @Override
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}

