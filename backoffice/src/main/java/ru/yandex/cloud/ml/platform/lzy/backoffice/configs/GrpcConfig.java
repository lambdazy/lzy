package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("grpc")
public class GrpcConfig {

    private String host;
    private int port;
    private String wbhost;
    private int wbport;

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

    public String getWbhost() {
        return wbhost;
    }

    public void setWbhost(String wbhost) {
        this.wbhost = wbhost;
    }

    public int getWbport() {
        return wbport;
    }

    public void setWbport(int wbport) {
        this.wbport = wbport;
    }
}
