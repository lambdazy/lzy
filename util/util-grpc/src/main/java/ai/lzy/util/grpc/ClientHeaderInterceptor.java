package ai.lzy.util.grpc;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Supplier;

public class ClientHeaderInterceptor<T> implements ClientInterceptor {
    private static final Logger LOG = LogManager.getLogger(ClientHeaderInterceptor.class);

    private final Metadata.Key<T> key;
    private final Supplier<T> value;

    public static ClientHeaderInterceptor<String> header(Metadata.Key<String> key, Supplier<String> value) {
        if (GrpcHeaders.AUTHORIZATION.equals(key)) {
            return new ClientHeaderInterceptor<>(GrpcHeaders.AUTHORIZATION, () -> "Bearer " + value.get());
        }

        return new ClientHeaderInterceptor<>(key, value);
    }

    public ClientHeaderInterceptor(Metadata.Key<T> key, Supplier<T> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                               CallOptions callOptions,
                                                               Channel channel)
    {
        final var call = channel.newCall(methodDescriptor, callOptions);

        return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
            public void start(Listener<RespT> responseListener, Metadata headers) {
                T v = value.get();
                if (v != null) {
                    LOG.trace("Attach header {}: {}", key, v);
                    headers.put(key, v);
                } else {
                    LOG.trace("Attach header {}: skip", key);
                }

                super.start(responseListener, headers);
            }
        };
    }
}
