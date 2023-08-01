package ai.lzy.service.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.util.kafka.KafkaConfig;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.annotation.Nullable;
import jakarta.validation.ValidationException;
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

    private OperationsConfig operations;

    private GarbageCollector gc;

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
    @ConfigurationProperties("operations")
    public static final class OperationsConfig {
        private volatile Duration startWorkflowTimeout;
        private volatile Duration finishWorkflowTimeout;
        private volatile Duration abortWorkflowTimeout;
        private volatile Duration executeGraphTimeout;
    }

    @Getter
    @Setter
    @ConfigurationProperties("gc")
    public static final class GarbageCollector {
        private boolean enabled;
        private Duration period;
        private Duration leaderPeriod;
    }

    @Getter
    @Setter
    @ConfigurationProperties("storage")
    public static final class StorageConfig {
        private S3Credentials s3;

        // legacy credentials format
        private YcCredentials yc;

        public void validate() {
            s3.validate();
        }

        @Getter
        @Setter
        @ConfigurationProperties("s3")
        public static final class S3Credentials {

            @ConfigurationBuilder("memory")
            private final InMemoryS3Credentials memory = new InMemoryS3Credentials();

            @ConfigurationBuilder("yc")
            private final YcS3Credentials yc = new YcS3Credentials();

            @ConfigurationBuilder("azure")
            private final AzureS3Credentials azure = new AzureS3Credentials();

            public void validate() {
                int cnt = (yc.enabled ? 1 : 0) + (azure.enabled ? 1 : 0) + (memory.enabled ? 1 : 0);

                if (cnt != 1) {
                    throw new ValidationException("Exactly one s3 provider should be enabled.");
                }
            }

            @Getter
            @Setter
            @ConfigurationProperties("yc")
            public static final class YcS3Credentials {
                private boolean enabled;
                private String endpoint;
                private String accessToken;
                private String secretToken;
            }

            @Getter
            @Setter
            @ConfigurationProperties("azure")
            public static final class AzureS3Credentials {
                private boolean enabled;
                private String connectionString;
            }

            @Getter
            @Setter
            @ConfigurationProperties("memory")
            public static class InMemoryS3Credentials {
                private boolean enabled = false;
                private int port;
            }
        }

        @Getter
        @Setter
        @ConfigurationProperties("yc")
        public static final class YcCredentials {
            private boolean enabled = false;
            private String endpoint;
            private String folderId;
        }
    }
}
