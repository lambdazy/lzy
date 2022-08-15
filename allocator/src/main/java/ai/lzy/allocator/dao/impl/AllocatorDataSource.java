package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.configs.DbConfig;
import ai.lzy.model.db.Storage;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Singleton
@Requires(property = "allocator.database.enabled", value = "true")
public class AllocatorDataSource implements Storage {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;

    @Inject
    public AllocatorDataSource(DbConfig config) {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(config.url());
        dataSource.setUser(config.username());
        dataSource.setPassword(config.password());

        dataSource.setMinPoolSize(config.minPoolSize());
        dataSource.setMaxPoolSize(config.maxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        var flyway = Flyway.configure()
            .dataSource(config.url(), config.username(), config.password())
            .locations("classpath:db/allocator/migrations")
            .load();
        flyway.migrate();
    }

    @Override
    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
