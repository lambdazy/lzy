package ai.lzy.worker;

import ai.lzy.util.kafka.KafkaConfig;
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
    private int gpuCount;
    private String iamAddress;

    @ConfigurationBuilder("kafka")
    private final KafkaConfig kafka = new KafkaConfig();
}
