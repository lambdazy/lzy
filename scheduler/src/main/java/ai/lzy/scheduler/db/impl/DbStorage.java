package ai.lzy.scheduler.db.impl;

import ai.lzy.scheduler.configs.DbConfig;
import ai.lzy.scheduler.db.Storage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Singleton
public class DbStorage implements Storage {
    private final DataSource dataSource;
    private final DbConfig config;

    @Inject
    public DbStorage(DataSource dataSource, DbConfig config) {
        this.config = config;
        var flyway = Flyway.configure()
            .dataSource(config.getUrl(), config.getUsername(), config.getPassword())
            .locations("classpath:db/scheduler/migrations")
            .load();
        flyway.migrate();

        this.dataSource = dataSource;
    }

    @Override
    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
