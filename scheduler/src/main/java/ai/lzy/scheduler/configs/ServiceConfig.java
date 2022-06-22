package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("service")
public record ServiceConfig(int executorsCount) {
}
