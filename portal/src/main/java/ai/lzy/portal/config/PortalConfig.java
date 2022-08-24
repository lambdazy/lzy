package ai.lzy.portal.config;

import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

@Getter
@Builder
public class PortalConfig {
    private final String servantId;
    private final String vmId;
    private final String allocatorAddress;
    private final int apiPort;
    private final int fsPort;
    private final String fsRoot;
    private final String channelManagerAddress;
    private final String host;
    private final String token;
    private final Duration allocatorHeartbeatPeriod;
    private final String stdoutChannelId;
    private final String stderrChannelId;
}
