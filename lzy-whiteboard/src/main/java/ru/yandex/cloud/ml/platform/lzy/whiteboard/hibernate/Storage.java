package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.DbConfig;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.EntryDependenciesModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardTagModel;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class Storage implements DbStorage {

    private final SessionFactory sessionFactory;

    @Inject
    public Storage(DbConfig config) {
        Flyway flyway = Flyway.configure()
            .dataSource(config.getUrl(), config.getUsername(), config.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", config.getUrl());
        cfg.setProperty("hibernate.connection.username", config.getUsername());
        cfg.setProperty("hibernate.connection.password", config.getPassword());
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.setProperty("hibernate.connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.addAnnotatedClass(EntryDependenciesModel.class);
        cfg.addAnnotatedClass(WhiteboardFieldModel.class);
        cfg.addAnnotatedClass(SnapshotEntryModel.class);
        cfg.addAnnotatedClass(WhiteboardModel.class);
        cfg.addAnnotatedClass(SnapshotModel.class);
        cfg.addAnnotatedClass(WhiteboardTagModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
