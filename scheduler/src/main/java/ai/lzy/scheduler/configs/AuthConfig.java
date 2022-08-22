package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("scheduler.auth")
public record AuthConfig(
    String serviceUid,
    String privateKey,
    String iamAddress
) {}
