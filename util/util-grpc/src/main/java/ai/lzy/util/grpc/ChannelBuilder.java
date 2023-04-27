package ai.lzy.util.grpc;

import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ChannelBuilder {

    public static final int IDLE_TIMEOUT_MINS = 5;
    public static final int KEEP_ALIVE_TIME_MINS = 3;
    public static final int KEEP_ALIVE_TIME_MINS_ALLOWED = 2;
    public static final int KEEP_ALIVE_TIMEOUT_SECS = 10;

    private final String host;
    private final int port;

    private boolean tls;
    private final List<String> retryServiceNames = new ArrayList<>();
    private int maxRetry;
    private String initialBackoff;
    private String maxBackoff;
    private double backoffMultiplier;

    private List<String> retryableStatusCodes;

    private ChannelBuilder(String host, int port) {
        this.host = host;
        this.port = port;

        this.tls = true;
        this.maxRetry = 3;
        this.initialBackoff = "0.5s";
        this.maxBackoff = "2s";
        this.backoffMultiplier = 2;
        this.retryableStatusCodes = List.of(
            Status.Code.ABORTED.name(),
            Status.Code.UNAVAILABLE.name(),
            Status.Code.DEADLINE_EXCEEDED.name(),
            Status.Code.RESOURCE_EXHAUSTED.name()
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

    @SuppressWarnings("UnstableApiUsage")
    public static ChannelBuilder forAddress(HostAndPort hostAndPort) {
        return new ChannelBuilder(hostAndPort.getHost(), hostAndPort.getPort());
    }

    public ChannelBuilder usePlaintext() {
        this.tls = false;
        return this;
    }

    public ChannelBuilder tls(boolean tls) {
        this.tls = tls;
        return this;
    }

    public ChannelBuilder enableRetry(String... services) {
        this.retryServiceNames.addAll(Arrays.asList(services));
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
        if (!retryServiceNames.isEmpty()) {
            configureRetry(builder, retryServiceNames);
        }
        return builder.build();
    }

    private void configureRetry(ManagedChannelBuilder<?> builder, Collection<String> serviceNames) {
        var retryPolicy = new HashMap<>();
        retryPolicy.put("maxAttempts", (double) maxRetry);
        retryPolicy.put("initialBackoff", initialBackoff);
        retryPolicy.put("maxBackoff", maxBackoff);
        retryPolicy.put("backoffMultiplier", backoffMultiplier);
        retryPolicy.put("retryableStatusCodes", retryableStatusCodes);

        builder.defaultServiceConfig(
            Map.of(
                "methodConfig", List.of(
                    Map.of(
                        "name", serviceNames.stream()
                            .map(serviceName -> Map.of("service", serviceName))
                            .toList(),
                        "retryPolicy", retryPolicy)
                )
            )
        );

        builder.enableRetry()
            .maxRetryAttempts(maxRetry)
            .maxHedgedAttempts(maxRetry);
    }

}
