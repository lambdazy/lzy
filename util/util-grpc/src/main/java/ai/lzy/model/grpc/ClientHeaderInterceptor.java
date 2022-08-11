package ai.lzy.model.grpc;

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
                                                               Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(channel.newCall(methodDescriptor, callOptions)) {
            public void start(Listener<RespT> responseListener, Metadata headers) {
                LOG.info("start key={} value={}", key, value.get());
                T v = ClientHeaderInterceptor.this.value.get();
                if (v != null) {
                    headers.put(ClientHeaderInterceptor.this.key, v);
                }

                super.start(responseListener, headers);
            }
        };
    }
}
