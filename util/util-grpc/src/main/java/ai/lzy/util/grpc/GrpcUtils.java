package ai.lzy.util.grpc;

import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.AbstractBlockingStub;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class GrpcUtils {

    public static final ServerInterceptor NO_AUTH = null;
    public static final Supplier<String>  NO_AUTH_TOKEN = null;

    private GrpcUtils() {}

    public static <T extends AbstractBlockingStub<T>> T newBlockingClient(T stub, String name,
                                                                          @Nullable Supplier<String> token)
    {
        if (token != null) {
            return stub.withInterceptors(
                GrpcLogsInterceptor.client(name),
                ClientHeaderInterceptor.authorization(token),
                RequestIdInterceptor.client());
        } else {
            return stub.withInterceptors(
                GrpcLogsInterceptor.client(name),
                RequestIdInterceptor.client());
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

    public static NettyServerBuilder intercept(NettyServerBuilder builder,
                                               @Nullable ServerInterceptor authInterceptor)
    {
        if (authInterceptor != null) {
            return builder
                .intercept(authInterceptor)
                .intercept(GrpcLogsInterceptor.server())
                .intercept(RequestIdInterceptor.server())
                .intercept(GrpcHeadersServerInterceptor.create());
        } else {
            return builder
                .intercept(GrpcLogsInterceptor.server())
                .intercept(RequestIdInterceptor.server())
                .intercept(GrpcHeadersServerInterceptor.create());
        }
    }

    public static NettyServerBuilder newGrpcServer(HostAndPort address, @Nullable ServerInterceptor authInterceptor) {
        return newGrpcServer(address.getHost(), address.getPort(), authInterceptor);
    }

    public static NettyServerBuilder newGrpcServer(String host, int port, @Nullable ServerInterceptor authInterceptor) {
        return intercept(
            addKeepAlive(
                NettyServerBuilder.forAddress(new InetSocketAddress(host, port))),
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
}
