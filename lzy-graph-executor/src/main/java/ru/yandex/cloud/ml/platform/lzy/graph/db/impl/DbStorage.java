package ru.yandex.cloud.ml.platform.lzy.graph.db.impl;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.beans.PropertyVetoException;
import org.flywaydb.core.Flyway;
import ru.yandex.cloud.ml.platform.lzy.graph.config.DbConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.db.Storage;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class DbStorage implements Storage {

    private final DbConfig dbConfig;
    private static final ComboPooledDataSource connectionPool = new ComboPooledDataSource();

    @Inject
    public DbStorage(DbConfig dbConfig) throws PropertyVetoException {
        Flyway flyway = Flyway.configure()
            .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
        this.dbConfig = dbConfig;

        connectionPool.setDriverClass("org.postgresql.Driver");
        connectionPool.setJdbcUrl(dbConfig.getUrl());
        connectionPool.setUser(dbConfig.getUsername());
        connectionPool.setPassword(dbConfig.getPassword());
        connectionPool.setMaxStatements(32);
    }

    @Override
    public synchronized Connection connect() throws SQLException {
        return connectionPool.getConnection();
    }
}
