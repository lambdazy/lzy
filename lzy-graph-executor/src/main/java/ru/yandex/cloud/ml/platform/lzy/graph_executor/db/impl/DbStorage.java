package ru.yandex.cloud.ml.platform.lzy.graph_executor.db.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.config.DbConfig;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.Storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class DbStorage implements Storage {
    private Connection connection;
    private final DbConfig dbConfig;

    @Inject
    public DbStorage(DbConfig dbConfig) {
        Flyway flyway = Flyway.configure()
            .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
        this.dbConfig = dbConfig;
    }

    @Override
    public synchronized Connection connect() throws SQLException {
        if (connection == null || !connection.isValid(1)) {
            connection = DriverManager.getConnection(
                    dbConfig.getUrl(),
                    dbConfig.getUsername(),
                    dbConfig.getPassword()
            );
        }
        return connection;
    }
}
