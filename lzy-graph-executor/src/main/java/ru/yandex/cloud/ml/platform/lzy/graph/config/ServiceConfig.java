package ru.yandex.cloud.ml.platform.lzy.graph.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("service")
public record ServiceConfig(int port, int executorsCount) {
}
