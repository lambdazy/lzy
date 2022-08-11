package ai.lzy.allocator.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("allocator")
public record ServiceConfig(
        int port,
        Duration gcPeriod,
        Duration allocationTimeout,
        Duration heartbeatTimeout,
        ThreadAllocator threadAllocator,
        DockerAllocator dockerAllocator,
        KuberAllocator kuberAllocator
) {
    @ConfigurationProperties("thread-allocator")
    public record ThreadAllocator(
        boolean enabled,
        String vmJarFile,
        String vmClassName
    ) {}

    @ConfigurationProperties("docker-allocator")
    public record DockerAllocator(
        boolean enabled
    ) {}

    @ConfigurationProperties("kuber-allocator")
    public record KuberAllocator(
        boolean enabled,
        String podTemplatePath
    ) {}
}
