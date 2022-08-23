package ai.lzy.graph.config;

import ai.lzy.iam.config.IamClientConfiguration;
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

    @ConfigurationBuilder("auth")
    private IamClientConfiguration auth = new IamClientConfiguration();

    @Getter
    @Setter
    @ConfigurationProperties("scheduler")
    public static class Scheduler {
        private String host;
        private int port;
    }
}
