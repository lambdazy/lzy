package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.net.URI;
import java.util.Map;

@ConfigurationProperties("scheduler")
public record ServiceConfig(
        int port,
        int maxServantsPerWorkflow,
        Map<String, Integer> provisioningLimits,
        Integer defaultProvisioningLimit,
        URI schedulerUri,
        URI whiteboardUri,
        String baseEnvDefaultImage,
        ThreadAllocator threadAllocator,
        DockerAllocator dockerAllocator
) {
    @ConfigurationProperties("thread-allocator")
    public record ThreadAllocator(
        boolean enabled,
        String filePath,
        String servantClassName
    ) {}

    @ConfigurationProperties("docker-allocator")
    public record DockerAllocator(
        boolean enabled
    ) {}
}
