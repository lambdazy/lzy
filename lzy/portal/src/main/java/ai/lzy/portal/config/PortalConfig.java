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
    private int slotsApiPort;

    private String channelManagerAddress;
    private String whiteboardAddress;

    private String iamAddress;
    private String iamPrivateKey;
    private String allocatorToken;

    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;

    private ConcurrencyConfig concurrency;

    @Getter
    @Setter
    @ConfigurationProperties("concurrency")
    public static final class ConcurrencyConfig {
        private int workersPoolSize = 10;
        private int downloadsPoolSize = 5;
        private int chunksPoolSize = 5;
    }

    public String toSafeString() {
        return "{ " + "portalId: " + portalId +
            ", host: " + host +
            ", portalPort: " + portalApiPort +
            ", slotsPort: " + slotsApiPort +
            ", vmId: " + vmId +
            " }";
    }
}
