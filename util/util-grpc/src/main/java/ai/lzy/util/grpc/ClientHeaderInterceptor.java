package ai.lzy.util.grpc;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Supplier;

public class ClientHeaderInterceptor<T> implements ClientInterceptor {
    private static final Logger LOG = LogManager.getLogger(ClientHeaderInterceptor.class);

    public record Entry<T>(
        Metadata.Key<T> key,
        Supplier<T> value
    ) {}

    private final List<Entry<T>> entries;

    public static ClientHeaderInterceptor<String> executionId(Supplier<String> value) {
        return new ClientHeaderInterceptor<>(GrpcHeaders.X_EXECUTION_ID, value);
    }

    public static ClientHeaderInterceptor<String> idempotencyKey(Supplier<String> value) {
        return new ClientHeaderInterceptor<>(GrpcHeaders.IDEMPOTENCY_KEY, value);
    }

    public static ClientHeaderInterceptor<String> authorization(Supplier<String> value) {
        return new ClientHeaderInterceptor<>(GrpcHeaders.AUTHORIZATION, () -> "Bearer " + value.get());
    }

    public static <T> ClientHeaderInterceptor<T> header(Metadata.Key<T> key, Supplier<T> value) {
        if (GrpcHeaders.AUTHORIZATION.equals(key)) {
            return cast(new ClientHeaderInterceptor<>(GrpcHeaders.AUTHORIZATION, () -> "Bearer " + value.get()));
        }

        return new ClientHeaderInterceptor<>(key, value);
    }

    public static <T> ClientHeaderInterceptor<T> all(List<Entry<T>> entries) {
        return new ClientHeaderInterceptor<>(entries);
    }

    public ClientHeaderInterceptor(Metadata.Key<T> key, Supplier<T> value) {
        this(List.of(new Entry<>(key, value)));
    }

    public ClientHeaderInterceptor(List<Entry<T>> entries) {
        this.entries = entries;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                               CallOptions callOptions,
                                                               Channel channel)
    {
        final var call = channel.newCall(methodDescriptor, callOptions);

        return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
            public void start(Listener<RespT> responseListener, Metadata headers) {
                for (var entry : entries) {
                    var v = entry.value().get();
                    if (v != null) {
                        LOG.trace("Attach header {}: {}", entry.key(), v);
                        headers.put(entry.key(), v);
                    } else {
                        LOG.trace("Attach header {}: skip", entry.key());
                    }
                }

                super.start(responseListener, headers);
            }
        };
    }

    private static <T> T cast(Object obj) {
        //noinspection unchecked
        return (T) obj;
    }
}
