package ai.lzy.whiteboard.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("service")
public class ServiceConfig {
    private String serverUri;
    private String iamUri;
    private int port;

    public String getServerUri() {
        return serverUri;
    }

    public void setServerUri(String serverUri) {
        this.serverUri = serverUri;
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
