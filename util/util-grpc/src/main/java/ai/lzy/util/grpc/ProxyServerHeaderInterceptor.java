package ai.lzy.util.grpc;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ProxyServerHeaderInterceptor implements ServerInterceptor {
    public static final Logger LOG = LogManager.getLogger(ProxyServerHeaderInterceptor.class);

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> call, Metadata headers,
                                                       ServerCallHandler<T, R> next)
    {
        Context context = Context.current().withValue(ProxyHeaderContext.KEY, new ProxyHeaderContext(headers));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
