package ai.lzy.scheduler.db.impl;

import ai.lzy.model.db.Storage;
import ai.lzy.scheduler.configs.DbConfig;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Singleton
public class SchedulerDataSource implements Storage {
    private final DataSource dataSource;
    private final DbConfig config;

    @Inject
    public SchedulerDataSource(DataSource dataSource, DbConfig config) {
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
