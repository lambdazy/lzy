package ai.lzy.service.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.storage.config.StorageClientConfiguration;
import ai.lzy.util.kafka.KafkaConfig;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties("lzy-service")
public class LzyServiceConfig {
    private String instanceId;

    private String address;
    private String allocatorAddress;
    private Duration allocatorVmCacheTimeout = Duration.ofMinutes(20);
    private String whiteboardAddress;
    private String graphExecutorAddress;
    private String channelManagerAddress;

    private Duration waitAllocationTimeout;
    private Duration gcPeriod;
    private Duration gcLeaderPeriod;

    private StartupPortalConfig portal;

    @Nullable
    private String s3SinkAddress = null;  // If not set, not using s3 sink

    @ConfigurationBuilder("kafka")
    private final KafkaConfig kafka = new KafkaConfig();

    @Getter
    @Setter
    @ConfigurationProperties("portal")
    public static final class StartupPortalConfig {
        private int portalApiPort;
        private int slotsApiPort;
        private String dockerImage;
        private String poolLabel;
        private String poolZone;
        private int workersPoolSize = 10;
        private int downloadsPoolSize = 5;
        private int chunksPoolSize = 5;
    }

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("storage")
    private final StorageClientConfiguration storage = new StorageClientConfiguration();

    @ConfigurationBuilder("operations")
    private final OperationsConfig operations = new OperationsConfig();


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
    public static final class OperationsConfig {
        private volatile Duration startWorkflowTimeout;
        private volatile Duration finishWorkflowTimeout;
        private volatile Duration abortWorkflowTimeout;
        private volatile Duration executeGraphTimeout;
    }
}
