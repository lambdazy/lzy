package ru.yandex.cloud.ml.platform.lzy.iam.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("iam")
public class ServiceConfig {
    private int userLimit;

    public int getUserLimit() {
        return userLimit;
    }

    public void setUserLimit(int userLimit) {
        this.userLimit = userLimit;
    }
}
