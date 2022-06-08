package ru.yandex.cloud.ml.platform.lzy.model.grpc;

import io.grpc.*;

import java.util.function.Supplier;

public class ClientHeaderInterceptor<T> implements ClientInterceptor {
    private final Metadata.Key<T> key;
    private final Supplier<T> value;

    public ClientHeaderInterceptor(Metadata.Key<T> key, Supplier<T> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> methodDescriptor,
            CallOptions callOptions,
            Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(channel.newCall(methodDescriptor, callOptions)) {
            public void start(Listener<RespT> responseListener, Metadata headers) {
                T v = ClientHeaderInterceptor.this.value.get();
                if (v != null) {
                    headers.put(ClientHeaderInterceptor.this.key, v);
                }

                super.start(responseListener, headers);
            }
        };
    }
}
