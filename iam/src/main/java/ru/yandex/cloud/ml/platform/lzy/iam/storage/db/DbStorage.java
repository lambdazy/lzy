package ru.yandex.cloud.ml.platform.lzy.iam.storage.db;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import ru.yandex.cloud.ml.platform.lzy.iam.configs.DbConfig;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class DbStorage {
    private static final Logger LOG = LogManager.getLogger(DbStorage.class);
    private Connection connection;
    private final DbConfig dbConfig;

    @Inject
    public DbStorage(DbConfig dbConfig) {
        Flyway flyway = Flyway.configure()
            .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
        this.dbConfig = dbConfig;
    }

    public synchronized Connection connect() throws SQLException {
        if (connection == null || !connection.isValid(1)) {
            connection = DriverManager.getConnection(
                    dbConfig.getUrl(),
                    dbConfig.getUsername(),
                    dbConfig.getPassword()
            );
        }
        return connection;
    }
}
