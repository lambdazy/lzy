package ai.lzy.allocator.storage;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.model.db.Storage;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
@Requires(property = "allocator.database.enabled", value = "true")
public class AllocatorDataSource implements Storage {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;

    @Inject
    public AllocatorDataSource(ServiceConfig config) {
        var dbConfig = config.getDatabase();

        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.getUrl());
        dataSource.setUser(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());

        dataSource.setMinPoolSize(dbConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(dbConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        var flyway = Flyway.configure()
            .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
            .locations("classpath:db/allocator/migrations")
            .load();
        flyway.migrate();
    }

    @Override
    public Connection connect() throws SQLException {
        var conn = dataSource.getConnection();
        conn.setAutoCommit(true);
        conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        return conn;
    }
}
