package ai.lzy.graph.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("graph-executor")
public class ServiceConfig {
    private int port;
    private int executorsCount;
    private Scheduler scheduler = new Scheduler();

    @ConfigurationBuilder("iam")
    private IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private DatabaseConfiguration database = new DatabaseConfiguration();

    @Getter
    @Setter
    @ConfigurationProperties("scheduler")
    public static class Scheduler {
        private String host;
        private int port;
    }
}
