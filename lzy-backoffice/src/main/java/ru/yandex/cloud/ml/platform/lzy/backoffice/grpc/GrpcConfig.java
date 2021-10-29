package ru.yandex.cloud.ml.platform.lzy.backoffice.grpc;

import io.micronaut.context.annotation.Configuration;
import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("grpc")
public class GrpcConfig {
    private String host;
    private int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
