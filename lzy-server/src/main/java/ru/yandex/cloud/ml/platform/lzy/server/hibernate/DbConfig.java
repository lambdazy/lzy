package ru.yandex.cloud.ml.platform.lzy.server.hibernate;


import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("database")
public class DbConfig {
    private String url;
    private String username;
    private String password;

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
