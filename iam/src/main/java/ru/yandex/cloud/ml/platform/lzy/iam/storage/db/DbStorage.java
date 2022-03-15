package ru.yandex.cloud.ml.platform.lzy.iam.storage.db;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.iam.configs.DbConfig;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.models.ResourceBindingModel;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class DbStorage {
    private static final Logger LOG = LogManager.getLogger(DbStorage.class);
    private final SessionFactory sessionFactory;

    @Inject
    public DbStorage(DbConfig dbConfig) {
        Flyway flyway = Flyway.configure()
            .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();

        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", dbConfig.getUrl());
        cfg.setProperty("hibernate.connection.username", dbConfig.getUsername());
        cfg.setProperty("hibernate.connection.password", dbConfig.getPassword());
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.setProperty("hibernate.connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");
        cfg.addAnnotatedClass(ResourceBindingModel.class);

        this.sessionFactory = cfg.buildSessionFactory();
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
