package ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.Storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Singleton
@Requires(missingProperty = "database.url")
@Requires(missingProperty = "database.username")
@Requires(missingProperty = "database.password")
public class DbStorageMock implements Storage {
    private Connection connection;
    private final String connectionUrl = "jdbc:h2:mem:testdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
    private final String dbUser = "test";

    public DbStorageMock() {
        Flyway flyway = Flyway.configure()
            .dataSource(connectionUrl, dbUser, "")
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
    }

    @Override
    public synchronized Connection connect() throws SQLException {
        if (connection == null || !connection.isValid(1)) {
            connection = DriverManager.getConnection(connectionUrl, dbUser, "");
        }
        return connection;
    }
}
