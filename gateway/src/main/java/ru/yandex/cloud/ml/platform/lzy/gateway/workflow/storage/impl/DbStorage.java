package ru.yandex.cloud.ml.platform.lzy.gateway.workflow.storage.impl;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;
import ru.yandex.cloud.ml.platform.lzy.gateway.workflow.configs.WorkflowDatabaseConfig;
import ru.yandex.cloud.ml.platform.lzy.gateway.workflow.storage.Storage;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
public class DbStorage implements Storage {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;

    @Inject
    public DbStorage(WorkflowDatabaseConfig dbConfig) {
        this.dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.getUrl());
        dataSource.setUser(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());

        dataSource.setMinPoolSize(dbConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(dbConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/workflow/migrations")
                .load();
        flyway.migrate();
    }

    @Override
    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
