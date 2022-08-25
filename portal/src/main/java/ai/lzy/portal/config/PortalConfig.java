package ai.lzy.portal.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
@Builder
@ConfigurationProperties("portal")
public class PortalConfig {
    private String portalId;

    private int apiPort;
    private String host;
    private String token;
    private String stdoutChannelId;
    private String stderrChannelId;

    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;

    private int fsPort;
    private String fsRoot;
    private String channelManagerAddress;
}
