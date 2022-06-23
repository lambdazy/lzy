package ai.lzy.model.utils;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.Constants;

public class SessionIdInterceptor implements ServerInterceptor {

    private static final Logger LOG = LogManager.getLogger(SessionIdInterceptor.class);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        final Metadata requestHeaders,
        ServerCallHandler<ReqT, RespT> next
    ) {
        LOG.debug("headers received from client:" + requestHeaders);
        final Context ctx = Context.current().withValue(
            Constants.SESSION_ID_CTX_KEY, requestHeaders.get(Constants.SESSION_ID_METADATA_KEY)
        );
        return Contexts.interceptCall(ctx, call, requestHeaders, next);
    }
}
