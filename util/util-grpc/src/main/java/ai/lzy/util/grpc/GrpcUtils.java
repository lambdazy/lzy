package ai.lzy.util.grpc;

import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;

import java.time.Duration;
import java.util.function.Supplier;

public final class GrpcUtils {

    private GrpcUtils() {}

    public static <T extends AbstractBlockingStub<T>> T newBlockingClient(T stub, String name, Supplier<String> token) {
        return stub.withInterceptors(
            GrpcLogsInterceptor.client(name),
            ClientHeaderInterceptor.authorization(token),
            RequestIdInterceptor.client());
    }

    public static <T extends AbstractBlockingStub<T>> T withIdempotencyKey(T stub, String idempotencyKey) {
        return stub.withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> idempotencyKey));
    }

    public static <T extends AbstractBlockingStub<T>> T withTimeout(T stub, Duration timeout) {
        return stub.withInterceptors(DeadlineClientInterceptor.fromDuration(timeout));
    }

    public static ManagedChannel newGrpcChannel(HostAndPort address, String serviceName) {
        return ChannelBuilder.forAddress(address)
            .usePlaintext()
            .enableRetry(serviceName)
            .build();
    }

    public static ManagedChannel newGrpcChannel(String host, int port, String serviceName) {
        return newGrpcChannel(HostAndPort.fromParts(host, port), serviceName);
    }

    public static ManagedChannel newGrpcChannel(String address, String serviceName) {
        return newGrpcChannel(HostAndPort.fromString(address), serviceName);
    }
}
