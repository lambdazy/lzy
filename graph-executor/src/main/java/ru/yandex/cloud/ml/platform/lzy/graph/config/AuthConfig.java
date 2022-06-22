package ru.yandex.cloud.ml.platform.lzy.graph.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("auth")
public record AuthConfig(String serviceUid, String privateKey, String iamHost, int iamPort) {}
