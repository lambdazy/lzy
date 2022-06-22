package ru.yandex.cloud.ml.platform.lzy.graph.db.impl;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import ru.yandex.cloud.ml.platform.lzy.graph.db.Storage;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
public class DbStorage implements Storage {
    private final DataSource dataSource;

    @Inject
    public DbStorage(DataSource dataSource) {
        var flyway = Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/graph/migrations")
            .load();
        flyway.migrate();

        this.dataSource = dataSource;
    }

    @Override
    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
