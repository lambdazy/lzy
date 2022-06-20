package ai.lzy.common.db;

import ai.lzy.configs.DbConfig;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import javax.sql.DataSource;

@Factory
public class DataSourceFactory {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    @Singleton
    @Requires(property = "database.url")
    @Requires(property = "database.username")
    @Requires(property = "database.password")
    ComboPooledDataSource dataSource(DbConfig dbConfig) {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.url());
        dataSource.setUser(dbConfig.username());
        dataSource.setPassword(dbConfig.password());

        dataSource.setMinPoolSize(dbConfig.minPoolSize());
        dataSource.setMaxPoolSize(dbConfig.maxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);
        return dataSource;
    }
}
