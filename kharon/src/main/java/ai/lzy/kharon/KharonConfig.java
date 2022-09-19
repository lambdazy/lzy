package ai.lzy.kharon;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.storage.config.StorageClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import javax.annotation.Nullable;

@Getter
@Setter
@ConfigurationProperties("kharon")
public class KharonConfig {
    private String address;
    @Nullable
    private String externalHost;
    private String serverAddress;
    private String whiteboardAddress;
    private String snapshotAddress;
    private String allocatorAddress;
    private String channelManagerAddress;
    private int servantProxyPort;
    private int servantFsProxyPort;
    private int channelManagerProxyPort;

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("storage")
    private final StorageClientConfiguration storage = new StorageClientConfiguration();

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    private WorkflowConfig workflow;

    @Getter
    @Setter
    @ConfigurationProperties("workflow")
    public static class WorkflowConfig {
        private boolean enabled;
        private Duration waitAllocationTimeout;
    }

    private PortalConfig portal;

    @Getter
    @Setter
    @ConfigurationProperties("portal")
    public static final class PortalConfig {
        private int portalApiPort;
        private int fsApiPort;
        private String fsRoot;
        private String portalImage;
        private String stdoutChannelName;
        private String stderrChannelName;
    }
}
