package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.DbConfig;
import ai.lzy.model.db.Storage;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

@Singleton
public class GraphExecutorDataSource implements Storage {
    private final DataSource dataSource;
    private final DbConfig config;

    @Inject
    public GraphExecutorDataSource(@Named("GraphExecutorDataSourceNative") DataSource dataSource, DbConfig config) {
        this.config = config;
        var flyway = Flyway.configure()
                .dataSource(config.getUrl(), config.getUsername(), config.getPassword())
                .locations("classpath:db/graph/migrations")
                .load();
        flyway.migrate();

        this.dataSource = dataSource;
    }

    @Override
    public Connection connect() throws SQLException {
        return dataSource.getConnection();
    }
}
