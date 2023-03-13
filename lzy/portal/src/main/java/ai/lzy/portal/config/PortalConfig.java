package ai.lzy.portal.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import javax.annotation.Nullable;

@Getter
@Setter
@ConfigurationProperties("portal")
public class PortalConfig {
    private String portalId;

    private String host;
    private int portalApiPort;
    private int slotsApiPort;

    @Nullable private String stdoutChannelId;
    @Nullable private String stderrChannelId;
    private String channelManagerAddress;
    private String whiteboardAddress;

    private String iamAddress;
    private String iamPrivateKey;
    private String allocatorToken;

    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;

    public String toSafeString() {
        return "{ " + "portalId: " + portalId +
            ", host: " + host +
            ", portalPort: " + portalApiPort +
            ", slotsPort: " + slotsApiPort +
            ", stdoutChannelId: " + stdoutChannelId +
            ", stderrChannelId: " + stderrChannelId +
            ", vmId: " + vmId +
            " }";
    }
}
