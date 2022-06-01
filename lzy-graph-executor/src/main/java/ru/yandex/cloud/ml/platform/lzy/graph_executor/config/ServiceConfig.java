package ru.yandex.cloud.ml.platform.lzy.graph_executor.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("service")
public class ServiceConfig {
    private int port;
    private int threadPoolSize = 16;
    private int processingPeriodMillis = 1000;
    private int batchSize = 100;

    public int port() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public int processingPeriodMillis() {
        return processingPeriodMillis;
    }

    public void setProcessingPeriodMillis(int processingPeriodMillis) {
        this.processingPeriodMillis = processingPeriodMillis;
    }

    public int batchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
