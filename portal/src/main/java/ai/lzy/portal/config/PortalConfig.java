package ai.lzy.portal.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

@Getter
@Setter
@ConfigurationProperties("portal")
public class PortalConfig {
    @NotBlank
    private String portalId;

    @NotNull
    @Positive
    private Integer portalApiPort;

    private String host;

    @NotNull
    private String token;

    @NotBlank
    private String stdoutChannelId;

    @NotBlank
    private String stderrChannelId;

    @NotNull
    @Positive
    private Integer fsApiPort;

    @NotNull
    private String fsRoot;

    @NotNull
    private String channelManagerAddress;
}
