package ai.lzy.allocator.configs;

import ai.lzy.allocator.alloc.impl.kuber.NetworkPolicyManager;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.tunnel.service.ValidationUtils;
import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.*;

@Getter
@Setter
@ConfigurationProperties("allocator")
public class ServiceConfig {
    private String instanceId;
    private List<String> hosts = new ArrayList<>(); // either [<ipv4>] or [<ipv4>,<ipv6>]
    private Integer port;
    private Duration allocationTimeout;
    private Duration mountTimeout = Duration.ofMinutes(2);
    private Duration heartbeatTimeout;
    private List<String> serviceClusters = new ArrayList<>();
    private List<String> userClusters = new ArrayList<>();
    private Set<NetworkPolicyManager.PolicyRule> serviceCidrs = new HashSet<>();
    private String nfsClientImage;

    public String getAddress() {
        String ipv6Host = hosts.stream().filter(ValidationUtils::validateIpV6).findFirst().orElse(null);
        String ipv4Host = hosts.stream().filter(ValidationUtils::validateIpV4).findFirst().orElse(null);

        // ipv6 has a higher priority than ipv4
        if (ipv6Host != null) {
            return HostAndPort.fromParts(ipv6Host, port).toString();
        }
        if (ipv4Host != null) {
            return HostAndPort.fromParts(ipv4Host, port).toString();
        }
        return HostAndPort.fromParts(hosts.get(0), port).toString();
    }

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    private boolean enableHttpDebug = false;

    @Getter
    @Setter
    @ConfigurationProperties("thread-allocator")
    public static final class ThreadAllocator {
        private boolean enabled = false;
        @Nullable
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
    @ConfigurationProperties("kuber-tunnel-allocator")
    public static final class KuberTunnelAllocator {
        private boolean enabled = false;
    }

    @Getter
    @Setter
    @ConfigurationProperties("yc-credentials")
    public static final class YcCredentialsConfig {
        private boolean enabled = false;
        private String iamEndpoint = "iam.api.cloud.yandex.net:443";
        private String endpoint = "api.cloud.yandex.net:443";
        private String serviceAccountFile;
    }

    @Getter
    @Setter
    @ConfigurationProperties("azure-credentials")
    public static final class AzureCredentialsConfig {
        private boolean enabled = false;
    }

    @Getter
    @Setter
    @ConfigurationProperties("disk-manager")
    public static final class DiskManagerConfig {
        private String folderId;
        private Duration defaultOperationTimeout;
    }

    public enum MetricsKind {
        Disabled,
        Logger,
        Prometheus,
    }

    @Getter
    @Setter
    @ConfigurationProperties("metrics")
    public static final class MetricsConfig {
        private MetricsKind kind = MetricsKind.Disabled;
        private int port = 17080;
        private String loggerName = "LogMetricReporter";
        private String loggerLevel = "info";
    }

    @Getter
    @Setter
    @ConfigurationProperties("gc")
    public static final class GcConfig {
        private Duration initialDelay = Duration.ofSeconds(10);
        private Duration cleanupPeriod = Duration.ofMinutes(5);
        private Duration leaseDuration = Duration.ofMinutes(30);
        private Duration gracefulShutdownDuration = Duration.ofSeconds(10);
    }

    @Getter
    @Setter
    @ConfigurationProperties("cache-limits")
    public static final class CacheLimits {
        private int userLimit = 5;
        private int sessionLimit = 3;
        @Nullable
        private Map<String, Integer> sessionPoolLimit = null;
        private int anySessionPoolLimit = 1;

        public int getLimit(String pool) {
            if (sessionPoolLimit == null) {
                return anySessionPoolLimit;
            }
            var limit = sessionPoolLimit.get(pool);
            if (limit == null) {
                return anySessionPoolLimit;
            }
            return limit;
        }
    }

    @Getter
    @Setter
    @ConfigurationProperties("tunnel")
    public static final class TunnelConfig {
        private String podImage;
        private String requestContainerImage;
        private String requestContainerGrpCurlPath;
        private int agentPort;
    }

    @Getter
    @Setter
    @ConfigurationProperties("mount")
    public static final class MountConfig {
        private boolean enabled = false;
        private String podImage;
        private String workerMountPoint;
        private String hostMountPoint;
    }
}
