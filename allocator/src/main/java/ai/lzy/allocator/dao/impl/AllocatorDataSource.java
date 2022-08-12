package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.configs.DbConfig;
import ai.lzy.model.db.Storage;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Singleton
public class AllocatorDataSource implements Storage {
    private final DataSource dataSource;
    private final DbConfig config;

    @Inject
    public AllocatorDataSource(@Named("AllocatorDataSourceNative") DataSource dataSource, DbConfig config) {
        this.config = config;
        var flyway = Flyway.configure()
            .dataSource(config.getUrl(), config.getUsername(), config.getPassword())
            .locations("classpath:db/allocator/migrations")
            .load();
        flyway.migrate();

        this.dataSource = dataSource;
    }

    @Override
    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
