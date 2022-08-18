package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties("scheduler")
public record ServiceConfig(
        int port,
        int maxServantsPerWorkflow,
        Map<String, Integer> provisioningLimits,
        Integer defaultProvisioningLimit,
        String schedulerAddress,
        String channelManagerAddress,
        String baseEnvDefaultImage,

        String allocatorAddress,
        String servantImage
) {
}
