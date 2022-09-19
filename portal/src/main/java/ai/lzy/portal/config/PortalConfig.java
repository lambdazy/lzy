package ai.lzy.portal.config;

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

    private String iamToken;
    private String allocatorToken;

    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;
}
