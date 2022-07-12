package ai.lzy.graph.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("graph-executor")
public record ServiceConfig(int port, int executorsCount, Scheduler scheduler) {

    @ConfigurationProperties("scheduler")
    public record Scheduler(String host, int port) {}
}
