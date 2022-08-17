package ai.lzy.allocator.configs;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.List;

// TODO(artolord) Optional fields are always empty
@Getter
@Setter
@ConfigurationProperties("allocator")
public class ServiceConfig {
    private String address;
    private Duration gcPeriod;
    private Duration allocationTimeout;
    private Duration heartbeatTimeout;
    private List<String> serviceClusters;
    private List<String> userClusters;

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    private ThreadAllocator threadAllocator = new ThreadAllocator();
    private DockerAllocator dockerAllocator = new DockerAllocator();
    private KuberAllocator kuberAllocator = new KuberAllocator();
    private YcMk8sConfig ycMk8s = new YcMk8sConfig();
    private AzureMk8sConfig azureMk8s = new AzureMk8sConfig();

    @Getter
    @Setter
    @ConfigurationProperties("thread-allocator")
    public static final class ThreadAllocator {
        private boolean enabled = false;
        private String vmJarFile;
        private String vmClassName;
    }

    @Getter
    @Setter
    @ConfigurationProperties("docker-allocator")
    public static final class DockerAllocator {
        private boolean enabled = false;
    }

    @Getter
    @Setter
    @ConfigurationProperties("kuber-allocator")
    public static final class KuberAllocator {
        private boolean enabled = false;
    }

    @Getter
    @Setter
    @ConfigurationProperties("yc-mk8s")
    public static final class YcMk8sConfig {
        private boolean enabled = false;
        private String serviceAccountFile;
    }

    @Getter
    @Setter
    @ConfigurationProperties("azure-mk8s")
    public static final class AzureMk8sConfig {
        private boolean enabled = false;
    }
}
