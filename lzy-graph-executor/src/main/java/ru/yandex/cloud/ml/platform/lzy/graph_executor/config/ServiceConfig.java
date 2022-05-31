package ru.yandex.cloud.ml.platform.lzy.graph_executor.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("service")
public class ServiceConfig {
    private int port;

    public int port() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
