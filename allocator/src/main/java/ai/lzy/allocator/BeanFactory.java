package ai.lzy.allocator;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.metrics.DummyMetricReporter;
import ai.lzy.metrics.LogMetricReporter;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.PrometheusMetricReporter;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.CollectorRegistry;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.Level;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;

import javax.inject.Named;
import java.nio.file.Path;
import java.time.Duration;

@Factory
public class BeanFactory {

    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    @Bean
    @Requires(property = "allocator.yc-credentials.enabled", value = "true")
    public ServiceFactory serviceFactory(ServiceConfig.YcCredentialsConfig config) {
        return ServiceFactory.builder()
            .credentialProvider(
                Auth.apiKeyBuilder()
                    .fromFile(Path.of(config.getServiceAccountFile()))
                    .build())
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();
    }

    @Singleton
    public ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean(preDestroy = "shutdown")
    @Named("AllocatorIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext() // TODO
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }

    @Bean
    @Requires(beans = ServiceConfig.MetricsConfig.class)
    public MetricReporter metricReporter(ServiceConfig.MetricsConfig config) {
        CollectorRegistry.defaultRegistry.clear();

        return switch (config.getKind()) {
            case Disabled -> new DummyMetricReporter();
            case Logger -> new LogMetricReporter(config.getLoggerName(),
                Level.valueOf(config.getLoggerLevel().toUpperCase()));
            case Prometheus -> new PrometheusMetricReporter(config.getPort());
        };
    }
}
