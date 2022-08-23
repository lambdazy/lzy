package ai.lzy.scheduler.configs;

import ai.lzy.iam.config.IamClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@ConfigurationProperties("scheduler")
public class ServiceConfig {
    private int port;
    private int maxServantsPerWorkflow;
    private Map<String, Integer> provisioningLimits;
    private Integer defaultProvisioningLimit;
    private String schedulerAddress;
    private String channelManagerAddress;

    private String allocatorAddress;
    private String servantImage;

    private boolean test = false;

    @ConfigurationBuilder("auth")
    private IamClientConfiguration auth = new IamClientConfiguration();
}
