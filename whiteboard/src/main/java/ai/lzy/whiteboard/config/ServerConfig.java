package ai.lzy.whiteboard.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("server")
public class ServerConfig {
    private String uri;
    private String iamUri;
    private int port;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getIamUri() {
        return iamUri;
    }

    public void setIamUri(String iamUri) {
        this.iamUri = iamUri;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}