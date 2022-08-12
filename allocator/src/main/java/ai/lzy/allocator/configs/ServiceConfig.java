package ai.lzy.allocator.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.exceptions.ConfigurationException;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

@ConfigurationProperties("allocator")
public record ServiceConfig(
    int port,
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

    @PostConstruct
    public void validate() {
        var cnt = ycMk8s.map(x -> x.enabled ? 1 : 0).orElse(0)
                + azureMk8s.map(x -> x.enabled ? 1 : 0).orElse(0);
        if (cnt != 1) {
            throw new ConfigurationException("Exactly one cloud provider should be specified.");
        }
    }

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
