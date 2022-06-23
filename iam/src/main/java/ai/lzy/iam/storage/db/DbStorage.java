package ai.lzy.iam.storage.db;

import ai.lzy.iam.configs.DbConfig;
import ai.lzy.iam.storage.Storage;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class DbStorage implements Storage {
    private static final Logger LOG = LogManager.getLogger(DbStorage.class);
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;

    @Inject
    public DbStorage(DbConfig dbConfig) {
        Flyway flyway = Flyway.configure()
                .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
                .locations("classpath:db/migrations")
                .load();
        flyway.migrate();

        this.dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.getUrl());
        dataSource.setUser(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());

        dataSource.setMinPoolSize(dbConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(dbConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);
    }

    @Override
    public synchronized Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
