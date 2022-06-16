package ru.yandex.cloud.ml.platform.lzy.model.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ChannelBuilder {

    public static final int IDLE_TIMEOUT_MINS = 5;
    public static final int KEEP_ALIVE_TIME_MINS = 3;
    public static final int KEEP_ALIVE_TIME_MINS_ALLOWED = 2;
    public static final int KEEP_ALIVE_TIMEOUT_SECS = 10;

    private final String host;
    private final int port;

    private boolean tls;
    private String retryServiceName;
    private int maxRetry;
    private String initialBackoff;
    private String maxBackoff;
    private double backoffMultiplier;

    private List<String> retryableStatusCodes;

    public ChannelBuilder(String host, int port) {
        this.host = host;
        this.port = port;

        this.tls = true;
        this.retryServiceName = null;
        this.maxRetry = 10;
        this.initialBackoff = "0.5s";
        this.maxBackoff = "30s";
        this.backoffMultiplier = 2;
        this.retryableStatusCodes = List.of(
            "CANCELLED",
            "UNKNOWN",
            "DEADLINE_EXCEEDED",
            "RESOURCE_EXHAUSTED",
            "FAILED_PRECONDITION",
            "ABORTED",
            "OUT_OF_RANGE",
            "INTERNAL",
            "UNAVAILABLE",
            "DATA_LOSS"
        );
    }

    public static ChannelBuilder forAddress(String host, int port) {
        return new ChannelBuilder(host, port);
    }

    public static ChannelBuilder forAddress(InetSocketAddress address) {
        return new ChannelBuilder(address.getHostName(), address.getPort());
    }

    public static ChannelBuilder forAddress(String hostPort) {
        int delimPos = hostPort.indexOf(':');
        var host = hostPort.substring(0, delimPos);
        var port = Integer.parseInt(hostPort.substring(delimPos + 1));
        return forAddress(host, port);
    }

    public ChannelBuilder usePlaintext() {
        this.tls = false;
        return this;
    }

    public ChannelBuilder tls(boolean tls) {
        this.tls = tls;
        return this;
    }

    public ChannelBuilder enableRetry(String service) {
        this.retryServiceName = service;
        return this;
    }

    public ChannelBuilder maxRetry(int retry) {
        this.maxRetry = retry;
        return this;
    }

    public ChannelBuilder initialBackoff(String initialBackoff) {
        this.initialBackoff = initialBackoff;
        return this;
    }

    public ChannelBuilder maxBackoff(String maxBackoff) {
        this.maxBackoff = maxBackoff;
        return this;
    }

    public ChannelBuilder backoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
        return this;
    }

    public ChannelBuilder retryableStatusCodes(List<String> retryableStatusCodes) {
        this.retryableStatusCodes = retryableStatusCodes;
        return this;
    }

    public ManagedChannel build() {
        var builder = ManagedChannelBuilder.forAddress(host, port);
        if (!tls) {
            builder.usePlaintext();
        }
        builder.keepAliveWithoutCalls(true)
            .idleTimeout(IDLE_TIMEOUT_MINS, TimeUnit.MINUTES)
            .keepAliveTime(KEEP_ALIVE_TIME_MINS, TimeUnit.MINUTES)
            .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECS, TimeUnit.SECONDS);
        if (retryServiceName != null) {
            configureRetry(builder, retryServiceName);
        }
        return builder.build();
    }

    private void configureRetry(ManagedChannelBuilder builder, String serviceName) {
        Map<String, Object> retryPolicy = new HashMap<>();
        retryPolicy.put("maxAttempts", (double) maxRetry);
        retryPolicy.put("initialBackoff", initialBackoff);
        retryPolicy.put("maxBackoff", maxBackoff);
        retryPolicy.put("backoffMultiplier", backoffMultiplier);
        retryPolicy.put("retryableStatusCodes", retryableStatusCodes);
        Map<String, Object> methodConfig = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("service", serviceName);
        methodConfig.put("name", Collections.<Object>singletonList(name));
        methodConfig.put("retryPolicy", retryPolicy);
        Map<String, Object> serviceConfig = new HashMap<>();
        serviceConfig.put("methodConfig", Collections.<Object>singletonList(methodConfig));
        builder.defaultServiceConfig(serviceConfig);

        builder.enableRetry()
            .maxRetryAttempts(maxRetry)
            .maxHedgedAttempts(maxRetry);

    }

}
