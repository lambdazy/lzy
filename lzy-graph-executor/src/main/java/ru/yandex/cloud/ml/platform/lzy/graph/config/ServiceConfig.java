package ru.yandex.cloud.ml.platform.lzy.graph.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("service")
public class ServiceConfig {
    private int port;
    private int executorsCount = 16;
    private int executionStepTimeoutSecs = 600;

    public int port() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int executorsCount() {
        return executorsCount;
    }

    public void setExecutorsCount(int executorsCount) {
        this.executorsCount = executorsCount;
    }

    public int executionStepTimeoutSecs() {
        return executionStepTimeoutSecs;
    }

    public void setExecutionStepTimeoutSecs(int executionStepTimeoutSecs) {
        this.executionStepTimeoutSecs = executionStepTimeoutSecs;
    }
}
