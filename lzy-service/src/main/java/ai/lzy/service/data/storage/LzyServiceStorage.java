package ai.lzy.service.data.storage;

import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.model.db.Storage;
import ai.lzy.service.config.LzyServiceConfig;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
@Requires(property = "lzy-service.database.url")
public class LzyServiceStorage implements Storage {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;

    public LzyServiceStorage(LzyServiceConfig configuration) {
        var databaseConfig = configuration.getDatabase();

        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(databaseConfig.getUrl());
        dataSource.setUser(databaseConfig.getUsername());
        dataSource.setPassword(databaseConfig.getPassword());

        dataSource.setMinPoolSize(databaseConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(databaseConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        var flyway = Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/lzy-service/migrations")
            .load();
        flyway.migrate();
    }

    @Override
    public Connection connect() throws SQLException {
        var connection = dataSource.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    @PreDestroy
    void close() {
        this.dataSource.close();
    }
}
