package ai.lzy.whiteboard.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("server")
public class ServerConfig {
    private String uri;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}