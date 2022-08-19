package ai.lzy.allocator;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;

import javax.inject.Named;
import java.nio.file.Path;
import java.time.Duration;

@Factory
public class BeansFactory {

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
    @Named("IamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext() // TODO
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }
}
