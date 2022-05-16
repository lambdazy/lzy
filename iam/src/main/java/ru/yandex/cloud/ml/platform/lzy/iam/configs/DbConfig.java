package ru.yandex.cloud.ml.platform.lzy.iam.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("database")
public class DbConfig {

    private String url;
    private String username;
    private String password;
    private boolean enabled;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
