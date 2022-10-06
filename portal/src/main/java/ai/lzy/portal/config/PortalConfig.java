package ai.lzy.portal.config;

import java.time.Duration;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

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

    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;
}
