package ai.lzy.portal.config;

import ai.lzy.iam.config.IamClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties("portal")
public class PortalConfig {
    private String portalId;

    private String host;
    private int portalApiPort;
    private int fsApiPort;
    private String fsRoot;

    private String stdoutChannelId;
    private String stderrChannelId;
    private String channelManagerAddress;

    private String iamAddress;
    private String iamPrivateKey;
    private String allocatorToken;

    // for tests
    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;
}
