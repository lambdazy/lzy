package ai.lzy.scheduler.configs;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.util.kafka.KafkaConfig;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties("scheduler")
public class ServiceConfig {
    private int port;
    private int maxWorkersPerWorkflow;
    private Map<String, Integer> provisioningLimits = new HashMap<>();
    private Integer defaultProvisioningLimit;
    private String schedulerAddress;
    private String channelManagerAddress;

    private String allocatorAddress;
    private String workerImage;
    private String userDefaultImage;

    @ConfigurationBuilder("iam")
    private IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("kafka")
    private KafkaConfig kafka = new KafkaConfig();
}
