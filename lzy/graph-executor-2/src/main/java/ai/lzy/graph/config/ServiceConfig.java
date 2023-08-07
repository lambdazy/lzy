package ai.lzy.graph.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.util.kafka.KafkaConfig;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("graph-executor-2")
public class ServiceConfig {
    private String instanceId;
    private int port;
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

