package ai.lzy.util.grpc;

import io.grpc.*;

import java.util.function.Supplier;

public final class HeaderAttachingClientInterceptor implements ClientInterceptor {
    private final Metadata.Key<String> key;
    private final Supplier<String> headerValue;

    public HeaderAttachingClientInterceptor(Metadata.Key<String> key, Supplier<String> headerValue) {
        this.key = key;
        this.headerValue = headerValue;
    }


    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next)
    {
        final var call = next.newCall(method, callOptions);

        return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(key, headerValue.get());
                super.start(responseListener, headers);
            }
        };
    }

}
