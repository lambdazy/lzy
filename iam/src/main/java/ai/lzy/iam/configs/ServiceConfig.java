package ai.lzy.iam.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("iam")
public class ServiceConfig {
    private int userLimit;
    private int serverPort;

    public int getUserLimit() {
        return userLimit;
    }

    public void setUserLimit(int userLimit) {
        this.userLimit = userLimit;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }
}
