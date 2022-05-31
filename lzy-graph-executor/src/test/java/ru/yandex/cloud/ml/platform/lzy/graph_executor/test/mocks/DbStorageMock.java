package ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.beans.PropertyVetoException;
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
    private static final ComboPooledDataSource connectionPool = new ComboPooledDataSource();

    public DbStorageMock() {
        final String connectionUrl = "jdbc:h2:mem:testdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        final String dbUser = "test";
        Flyway flyway = Flyway.configure()
            .dataSource(connectionUrl, dbUser, "")
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
        try {
            connectionPool.setDriverClass("org.h2.Driver");
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        connectionPool.setJdbcUrl(connectionUrl);
        connectionPool.setUser(dbUser);
        connectionPool.setPassword("");
        connectionPool.setMaxStatements(32);
    }

    @Override
    public Connection connect() throws SQLException {
        return connectionPool.getConnection();
    }
}
