package ai.lzy.graph.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
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
    private int userLimit;
    private int workflowLimit;

    @ConfigurationBuilder("iam")
    private IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private DatabaseConfiguration database = new DatabaseConfiguration();
}

