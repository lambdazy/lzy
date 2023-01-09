package ai.lzy.service.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.storage.config.StorageClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
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

    @Getter
    @Setter
    @ConfigurationProperties("portal")
    public static final class StartupPortalConfig {
        private int portalApiPort;
        private int slotsApiPort;
        private String dockerImage;
        private String stdoutChannelName;
        private String stderrChannelName;
    }

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("storage")
    private final StorageClientConfiguration storage = new StorageClientConfiguration();
}
