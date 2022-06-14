package ru.yandex.cloud.ml.platform.lzy.gateway.workflow.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("workflows")
public class WorkflowServiceConfig {
    private String serverAddress;

    public String getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }
}
