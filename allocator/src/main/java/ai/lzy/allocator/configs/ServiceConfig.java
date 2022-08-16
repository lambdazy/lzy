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

    DiskManagerConfig diskManagerConfig,

    Optional<YcCredentialsConfig> ycCredentialsConfig,
    Optional<AzureCredentialsConfig> azureCredentialsConfig,

    Iam iam
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

    @ConfigurationProperties("yc-credentials")
    public record YcCredentialsConfig(
        boolean enabled,
        String serviceAccountFile
    ) {}

    @ConfigurationProperties("azure-credentials")
    public record AzureCredentialsConfig(
        boolean enabled
    ) {}

    @ConfigurationProperties("disk-manager")
    public record DiskManagerConfig(
        String folderId,
        Duration defaultOperationTimeout
    ) {}

    @ConfigurationProperties("iam")
    public record Iam(
        String address,
        IamInternal internal
    ) {}

    @ConfigurationProperties("iam.internal")
    public record IamInternal(
        String userName,
        String credentialPrivateKey
    ) {}
}
