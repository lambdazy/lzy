package ai.lzy.scheduler.db.impl;

import ai.lzy.model.db.Storage;
import ai.lzy.scheduler.configs.DbConfig;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

@Singleton
public class SchedulerDataSource implements Storage {
    private final DataSource dataSource;
    private final DbConfig config;

    @Inject
    public SchedulerDataSource(@Named("SchedulerDataSource") DataSource dataSource, DbConfig config) {
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
