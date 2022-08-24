package ai.lzy.portal.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties("portal")
public class PortalConfig {
    private String servantId;
    private String vmId;
    private String token;
    private String host;
    private String channelManagerAddress;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;
    private int apiPort;
    private int fsPort;
    private String fsRoot;
    private String stdoutChannelId;
    private String stderrChannelId;
}
