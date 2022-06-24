package ai.lzy.graph.db;

import ai.lzy.graph.config.DbConfig;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import javax.sql.DataSource;

@Factory
public class DataSourceFactory {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    @Singleton
    @Requires(property = "database.enabled", value = "true")
    DataSource dataSource(DbConfig dbConfig) {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.getUrl());
        dataSource.setUser(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());

        dataSource.setMinPoolSize(dbConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(dbConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);
        return dataSource;
    }
}
