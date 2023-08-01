package ai.lzy.service.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
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

    @ConfigurationBuilder("gc")
    private final GarbageCollector gc = new GarbageCollector();

    private Duration bucketCreationTimeout;

    @Nullable
    private String s3SinkAddress = null;  // If not set, not using s3 sink

    @ConfigurationBuilder("kafka")
    private final KafkaConfig kafka = new KafkaConfig();

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("storage")
    private final StorageConfig storage = new StorageConfig();

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

    @Getter
    @Setter
    public static final class GarbageCollector {
        private boolean enabled;
        private Duration period;
        private Duration leaderPeriod;
    }
}
