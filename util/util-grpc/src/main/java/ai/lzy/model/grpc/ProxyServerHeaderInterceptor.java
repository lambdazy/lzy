package ai.lzy.model.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ProxyServerHeaderInterceptor implements ServerInterceptor {
    public static final Logger LOG = LogManager.getLogger(ProxyServerHeaderInterceptor.class);

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> call, Metadata headers,
                                                       ServerCallHandler<T, R> next) {
        Context context = Context.current().withValue(ProxyHeaderContext.KEY, new ProxyHeaderContext(headers));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
