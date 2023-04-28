package ai.lzy.kafka;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.util.kafka.KafkaConfig;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("s3-sink")
public class ServiceConfig {
    private String address;

    @ConfigurationBuilder("kafka")
    private final KafkaConfig kafka = new KafkaConfig();

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();
}
