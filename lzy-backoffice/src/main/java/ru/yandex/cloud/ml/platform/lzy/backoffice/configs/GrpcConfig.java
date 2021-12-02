package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("grpc")
public class GrpcConfig {

    private String host;
    private int port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
