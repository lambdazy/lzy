package ai.lzy.util.grpc;

import com.google.common.net.HostAndPort;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import org.apache.logging.log4j.ThreadContext;

import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class GrpcUtils {

    private GrpcUtils() {}

    public static <T extends AbstractBlockingStub<T>> T newBlockingClient(T stub, Supplier<String> token) {
        return stub.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, token),
            ClientHeaderInterceptor.header(GrpcHeaders.X_REQUEST_ID, GrpcUtils::getGrpcRequestId)
        );
    }

    public static <T extends AbstractBlockingStub<T>> T newBlockingClient(Function<Channel, T> factory, String service,
                                                                          String address, Supplier<String> token)
    {
        return factory.apply(newGrpcChannel(address, service))
            .withInterceptors(
                ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, token),
                ClientHeaderInterceptor.header(GrpcHeaders.X_REQUEST_ID, GrpcUtils::getGrpcRequestId));
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

    private static final String LOG_PROP_REQID = "reqid";

    @Nullable
    public static String getGrpcRequestId() {
        var reqid = GrpcHeaders.getRequestId();
        if (reqid != null) {
            return reqid;
        }
        return ThreadContext.get(LOG_PROP_REQID);
    }

    public static void attachGrpcRequestId() {
        ThreadContext.put(LOG_PROP_REQID, getGrpcRequestId());
    }

    public static void detachGrpcRequestId() {
        ThreadContext.remove(LOG_PROP_REQID);
    }

    public static final class GrpcRequestIdHolder implements AutoCloseable {
        public static GrpcRequestIdHolder init() {
            return new GrpcRequestIdHolder();
        }

        public GrpcRequestIdHolder() {
            attachGrpcRequestId();
        }

        @Override
        public void close() throws Exception {
            detachGrpcRequestId();
        }
    }
}
