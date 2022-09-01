package ai.lzy.kharon;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.storage.config.StorageClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.http.annotation.Get;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

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
    private String channelManagerAddress;
    private String allocatorAddress;
    private String operationServiceAddress;
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
    }

    private PortalConfig portal;

    @Getter
    @Setter
    @ConfigurationProperties("portal")
    public static final class PortalConfig {
        private String host;
        private Integer portalApiPort;
        private Integer fsApiPort;
        private String fsRoot;

        private String portalImage;

        @NotBlank
        private String stdoutChannelId;

        @NotBlank
        private String stderrChannelId;
    }
}
