package ru.yandex.cloud.ml.platform.lzy.gateway.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("gateway")
public class GatewayServiceConfig {
    private int port;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
