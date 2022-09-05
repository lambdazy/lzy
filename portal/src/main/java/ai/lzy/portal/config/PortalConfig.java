package ai.lzy.portal.config;

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
    private String token;
    private String stdoutChannelId;
    private String stderrChannelId;
    private int fsApiPort;
    private String fsRoot;
    private String channelManagerAddress;
}
