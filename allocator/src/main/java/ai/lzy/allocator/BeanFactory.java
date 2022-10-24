package ai.lzy.allocator;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.metrics.DummyMetricReporter;
import ai.lzy.metrics.LogMetricReporter;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.PrometheusMetricReporter;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
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
import yandex.cloud.sdk.auth.provider.CredentialProvider;

import java.nio.file.Path;
import java.time.Duration;
import javax.inject.Named;

@Factory
public class BeanFactory {

    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    @Singleton
    @Requires(property = "allocator.yc-credentials.enabled", value = "true")
    public ServiceFactory serviceFactory(
            CredentialProvider credentialProvider, ServiceConfig.YcCredentialsConfig config)
    {
        return ServiceFactory.builder()
            .credentialProvider(credentialProvider)
            .endpoint(config.getEndpoint())
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();
    }

    @Singleton
    @Requires(property = "allocator.yc-credentials.enabled", value = "true")
    public CredentialProvider credentialProvider(ServiceConfig.YcCredentialsConfig config) {
        return Auth.apiKeyBuilder()
            .fromFile(Path.of(config.getServiceAccountFile()))
            .cloudIAMEndpoint(config.getIamEndpoint())
            .build();
    }

    @Singleton
    public ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return GrpcUtils.newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
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

    @Singleton
    public RenewableJwt renewableIamToken(ServiceConfig config) {
        return config.getIam().createRenewableToken();
    }
}
