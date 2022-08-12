package ai.lzy.allocator.vmpool.yc;

import ai.lzy.allocator.configs.ServiceConfig;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;

import java.nio.file.Path;
import java.time.Duration;

@Factory
public class BeansFactory {

    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    @Bean
    public ServiceFactory serviceFactory(ServiceConfig.YcMk8sConfig config) {
        return ServiceFactory.builder()
            .credentialProvider(
                Auth.apiKeyBuilder()
                    .fromFile(Path.of(config.serviceAccountFile()))
                    .build())
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();
    }
}
