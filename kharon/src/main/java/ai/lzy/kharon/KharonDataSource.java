package ai.lzy.kharon;

import ai.lzy.model.db.Storage;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
@Requires(property = "kharon.database.url")
public class KharonDataSource implements Storage {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;

    @Inject
    public KharonDataSource(KharonConfig.DatabaseConfig dbConfig) {
        this.dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.url());
        dataSource.setUser(dbConfig.username());
        dataSource.setPassword(dbConfig.password());

        dataSource.setMinPoolSize(dbConfig.minPoolSize());
        dataSource.setMaxPoolSize(dbConfig.maxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/kharon/migrations")
                .load();
        flyway.migrate();
    }

    @Override
    public Connection connect() throws SQLException {
        var conn = dataSource.getConnection();
        conn.setAutoCommit(true);
        return conn;
    }
}
