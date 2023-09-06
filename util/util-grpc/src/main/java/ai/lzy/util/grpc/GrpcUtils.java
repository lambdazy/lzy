package ai.lzy.util.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.AbstractBlockingStub;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class GrpcUtils {
    public static final ServerInterceptor NO_AUTH = null;
    public static final Supplier<String> NO_AUTH_TOKEN = null;

    public static final AtomicBoolean IS_RETRIES_ENABLED = new AtomicBoolean(true);
    public static final RetryConfig INFINITY_RETRY_CONFIG = new RetryConfig(
        /* infinity */ 0, e -> retryableStatusCode(e.getStatus()), Duration.ofMillis(100), Duration.ofSeconds(5), 1.3);
    public static final RetryConfig LONGENOUGH_RETRY_CONFIG = new RetryConfig(
        200, e -> retryableStatusCode(e.getStatus()), Duration.ofMillis(100), Duration.ofSeconds(5), 1.3); // ~15 min

    private static final List<ClientHeaderInterceptor.Entry<String>> COMMON_CLIENT_HEADERS = List.of(
        new ClientHeaderInterceptor.Entry<>(GrpcHeaders.X_REQUEST_ID, GrpcHeaders::getRequestId),
        new ClientHeaderInterceptor.Entry<>(GrpcHeaders.X_EXECUTION_ID, GrpcHeaders::getExecutionId),
        new ClientHeaderInterceptor.Entry<>(GrpcHeaders.X_EXECUTION_TASK_ID, GrpcHeaders::getExecutionTaskId)
    );

    private GrpcUtils() {
    }

    public static <T extends AbstractBlockingStub<T>> T newBlockingClient(T stub, String name,
                                                                          @Nullable Supplier<String> token)
    {
        if (token != null) {
            return stub.withInterceptors(
                GrpcLogsInterceptor.client(name),
                ClientHeaderInterceptor.authorization(token),
                ClientHeaderInterceptor.all(COMMON_CLIENT_HEADERS));
        } else {
            return stub.withInterceptors(
                GrpcLogsInterceptor.client(name),
                ClientHeaderInterceptor.all(COMMON_CLIENT_HEADERS));
        }
    }

    public static <T extends AbstractBlockingStub<T>> T withIdempotencyKey(T stub, String idempotencyKey) {
        return stub.withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> idempotencyKey));
    }

    public static <T extends AbstractBlockingStub<T>> T withTimeout(T stub, Duration timeout) {
        return stub.withInterceptors(DeadlineClientInterceptor.fromDuration(timeout));
    }

    public static ManagedChannel newGrpcChannel(HostAndPort address, String... serviceNames) {
        return ChannelBuilder.forAddress(address)
            .usePlaintext()
            .enableRetry(serviceNames)
            .build();
    }

    public static ManagedChannel newGrpcChannel(String host, int port, String... serviceNames) {
        return newGrpcChannel(HostAndPort.fromParts(host, port), serviceNames);
    }

    public static ManagedChannel newGrpcChannel(String address, String... serviceNames) {
        return newGrpcChannel(HostAndPort.fromString(address), serviceNames);
    }

    public static NettyServerBuilder addKeepAlive(NettyServerBuilder builder) {
        return builder
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);
    }

    public static NettyServerBuilder addReflection(NettyServerBuilder builder) {
        return builder
            .addService(ProtoReflectionService.newInstance());
    }

    public static NettyServerBuilder intercept(NettyServerBuilder builder,
                                               @Nullable ServerInterceptor authInterceptor)
    {
        if (authInterceptor != null) {
            return builder
                .intercept(authInterceptor)
                .intercept(GrpcLogsInterceptor.server())
                .intercept(RequestIdInterceptor.forward())
                .intercept(RemoteAddressInterceptor.create())
                .intercept(GrpcHeadersServerInterceptor.create());
        } else {
            return builder
                .intercept(GrpcLogsInterceptor.server())
                .intercept(RequestIdInterceptor.forward())
                .intercept(RemoteAddressInterceptor.create())
                .intercept(GrpcHeadersServerInterceptor.create());
        }
    }

    public static NettyServerBuilder newGrpcServer(HostAndPort address, @Nullable ServerInterceptor authInterceptor) {
        return newGrpcServer(address.getHost(), address.getPort(), authInterceptor);
    }

    public static NettyServerBuilder newGrpcServer(String host, int port, @Nullable ServerInterceptor authInterceptor) {
        return intercept(
            addReflection(
                addKeepAlive(
                    NettyServerBuilder.forAddress(new InetSocketAddress(host, port)))),
            authInterceptor);
    }

    public static StatusRuntimeException mapToGrpcException(Exception e) {
        if (e instanceof StatusRuntimeException statusRuntimeException) {
            return statusRuntimeException;
        }
        if (e instanceof StatusException statusException) {
            return new StatusRuntimeException(statusException.getStatus(), statusException.getTrailers());
        }
        if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
        }
        if (e instanceof IllegalStateException) {
            return Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException();
        }
        if (e instanceof UnsupportedOperationException) {
            return Status.UNIMPLEMENTED.withDescription(e.getMessage()).asRuntimeException();
        }
        if (e instanceof RuntimeException) {
            return Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
        return Status.UNKNOWN.withDescription(e.getMessage()).asRuntimeException();
    }

    public static boolean retryableStatusCode(Status sre) {
        return switch (sre.getCode()) {
            case UNAVAILABLE, ABORTED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED -> true;
            default -> false;
        };
    }

    public record RetryConfig(
        int count,  // Number of retries, or 0 for infinity
        Predicate<StatusRuntimeException> isRetryable,
        Duration initialBackoff,
        Duration maxBackoff,
        double backoffMultiplayer
    ) {}

    @FunctionalInterface
    public interface GrpcCall<T> {
        T call() throws StatusRuntimeException;
    }

    @FunctionalInterface
    public interface GrpcRun {
        void run() throws StatusRuntimeException;
    }

    public static <T> T withRetries(Logger logger, RetryConfig config, GrpcCall<T> func) throws StatusRuntimeException {
        var attempt = 0;
        var delayMs = 0L;

        while (true) {
            if (++attempt != 1) {
                if (delayMs == 0) {
                    Runtime.getRuntime().halt(42);
                }
                LockSupport.parkNanos(Duration.ofMillis(delayMs).toNanos());
            }

            try {
                return func.call();
            } catch (StatusRuntimeException e) {
                if (!config.isRetryable.test(e)) {
                    logger.error("Got not retryable error while executing some grpc call: ", e);
                    throw e;
                }

                boolean retry = IS_RETRIES_ENABLED.get();
                if (retry && config.count != 0) {
                    retry = attempt <= config.count;
                }

                if (retry) {
                    if (attempt == 1) {
                        delayMs = config.initialBackoff.toMillis();
                    } else {
                        delayMs = Math.min(config.maxBackoff.toMillis(), (long) (delayMs * config.backoffMultiplayer));
                    }
                    logger.warn("Got retryable error while executing some grpc call, retry after {} ms", delayMs, e);
                    continue;
                }

                logger.error("Got retryable error while executing some grpc call, but retry count exceeded: ", e);
                throw e;
            }
        }
    }

    public static <T> T withRetries(Logger logger, GrpcCall<T> func) throws StatusRuntimeException {
        return withRetries(logger, INFINITY_RETRY_CONFIG, func);
    }

    public static void withRetries(Logger logger, GrpcRun func) throws StatusRuntimeException {
        withRetries(logger, INFINITY_RETRY_CONFIG, () -> {
            func.run();
            return null;
        });
    }

    @VisibleForTesting
    public static void setIsRetriesEnabled(boolean enabled) {
        IS_RETRIES_ENABLED.set(enabled);
    }
}
