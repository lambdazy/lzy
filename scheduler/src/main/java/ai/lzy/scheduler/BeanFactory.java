package ai.lzy.scheduler;

import ai.lzy.scheduler.configs.DbConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import javax.inject.Named;
import javax.sql.DataSource;

@Factory
public class BeanFactory {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    @Singleton
    @Requires(property = "scheduler.database.enabled", value = "true")
    @Named("SchedulerDataSource")
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

    @Bean(preDestroy = "shutdown")
    @Named("IamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return ChannelBuilder
            .forAddress(config.getAuth().getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }
}
