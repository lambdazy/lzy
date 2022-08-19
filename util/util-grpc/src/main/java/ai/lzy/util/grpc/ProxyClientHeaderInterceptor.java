package ai.lzy.util.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProxyClientHeaderInterceptor implements ClientInterceptor {
    private static final Logger LOG = LogManager.getLogger(ProxyClientHeaderInterceptor.class);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                               CallOptions callOptions,
                                                               Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(channel.newCall(methodDescriptor, callOptions)) {
            public void start(Listener<RespT> responseListener, Metadata headers) {
                final ProxyHeaderContext proxyHeaderContext = ProxyHeaderContext.current();
                if (proxyHeaderContext != null) {
                    final Metadata currentHeaders = proxyHeaderContext.headers();
                    headers.merge(currentHeaders);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
