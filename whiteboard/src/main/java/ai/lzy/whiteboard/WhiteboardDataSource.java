package ai.lzy.whiteboard;

import ai.lzy.model.db.Storage;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;

@Singleton
@Requires(property = "whiteboard.database.url")
@Requires(property = "whiteboard.database.username")
@Requires(property = "whiteboard.database.password")
public class WhiteboardDataSource implements Storage {

    private static final Logger LOG = LogManager.getLogger(WhiteboardDataSource.class);
    private static final String VALIDATION_QUERY_SQL = "select 1";
    private static final String MIGRATIONS_LOCATION = "classpath:db/whiteboard/migrations";

    private final ComboPooledDataSource dataSource;

    @Inject
    public WhiteboardDataSource(WhiteboardConfig.DbConfig dbConfig) {

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.getUrl());
        dataSource.setUser(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());

        dataSource.setMinPoolSize(dbConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(dbConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        Flyway flyway = Flyway.configure()
            .dataSource(dataSource)
            .locations(MIGRATIONS_LOCATION)
            .load();
        flyway.migrate();

        this.dataSource = dataSource;
    }

    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
