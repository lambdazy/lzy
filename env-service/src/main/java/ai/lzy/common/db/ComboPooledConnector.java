package ai.lzy.common.db;

import ai.lzy.configs.DbConfig;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;

@Singleton
public class ComboPooledConnector implements DbConnector {

    private static final Logger LOG = LogManager.getLogger(ComboPooledConnector.class);

    private final ComboPooledDataSource dataSource;

    @Inject
    public ComboPooledConnector(ComboPooledDataSource dataSource) {
        Flyway flyway = Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/env/migrations")
            .load();
        flyway.migrate();

        this.dataSource = dataSource;
    }

    @Override
    public synchronized Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
