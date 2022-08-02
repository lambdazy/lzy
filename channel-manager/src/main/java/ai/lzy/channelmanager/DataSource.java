package ai.lzy.channelmanager;

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
@Requires(property = "disk-service.database.url")
@Requires(property = "disk-service.database.username")
@Requires(property = "disk-service.database.password")
public class DataSource implements Storage {

    private static final Logger LOG = LogManager.getLogger(DataSource.class);
    private static final String VALIDATION_QUERY_SQL = "select 1";
    private static final String MIGRATIONS_LOCATION = "classpath:db/channel-manager/migrations";

    private final ComboPooledDataSource dataSource;

    @Inject
    public DataSource(ChannelManagerConfig.DbConfig dbConfig) {

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.url());
        dataSource.setUser(dbConfig.username());
        dataSource.setPassword(dbConfig.password());

        dataSource.setMinPoolSize(dbConfig.minPoolSize());
        dataSource.setMaxPoolSize(dbConfig.maxPoolSize());

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
