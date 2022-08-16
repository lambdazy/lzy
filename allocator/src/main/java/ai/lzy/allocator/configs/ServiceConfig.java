package ai.lzy.allocator.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

// TODO(artolord) Optional fields are always empty
@ConfigurationProperties("allocator")
public record ServiceConfig(
    String address,
    Duration gcPeriod,
    Duration allocationTimeout,
    Duration heartbeatTimeout,

    List<String> serviceClusters,
    List<String> userClusters,

    Optional<ThreadAllocator> threadAllocator,
    Optional<DockerAllocator> dockerAllocator,
    Optional<KuberAllocator> kuberAllocator,

    Optional<YcMk8sConfig> ycMk8s,
    Optional<AzureMk8sConfig> azureMk8s
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
        boolean enabled
    ) {}

    @ConfigurationProperties("yc-mk8s")
    public record YcMk8sConfig(
        boolean enabled,
        String serviceAccountFile
    ) {}

    @ConfigurationProperties("azure-mk8s")
    public record AzureMk8sConfig(
        boolean enabled
    ) {}
}
