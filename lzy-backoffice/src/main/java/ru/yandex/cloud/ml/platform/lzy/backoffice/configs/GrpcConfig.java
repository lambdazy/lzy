package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("grpc")
public class GrpcConfig {

    private String host;
    private int port;
    private String wbHost;
    private int wbPort;

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

    public String getWbHost() {
        return wbHost;
    }

    public void setWbHost(String wbHost) {
        this.wbHost = wbHost;
    }

    public int getWbPort() {
        return wbPort;
    }

    public void setWbPort(int wbPort) {
        this.wbPort = wbPort;
    }
}
