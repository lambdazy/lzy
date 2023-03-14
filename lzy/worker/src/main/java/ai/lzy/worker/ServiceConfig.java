package ai.lzy.worker;

import ai.lzy.iam.config.IamClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties("worker")
public class ServiceConfig {
    private String vmId;
    private String allocatorAddress;
    private Duration allocatorHeartbeatPeriod;
    private String channelManagerAddress;
    private String host;
    private String allocatorToken;
    private int fsPort;
    private int apiPort;
    private String mountPoint;
    private String publicKey;
    private int gpuCount;

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();
}
