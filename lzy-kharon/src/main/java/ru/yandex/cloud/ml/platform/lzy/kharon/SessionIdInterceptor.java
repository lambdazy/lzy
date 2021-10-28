package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.*;

public class SessionIdInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor,
        CallOptions callOptions, Channel channel
    ) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(Constant.SESSION_ID_METADATA_KEY, Constant.SESSION_ID_CTX_KEY.get());
                super.start(responseListener, headers);
            }
        };
    }
}
