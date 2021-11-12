package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.GrpcConstant;

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
            GrpcConstant.SESSION_ID_CTX_KEY, requestHeaders.get(GrpcConstant.SESSION_ID_METADATA_KEY)
        );
        return Contexts.interceptCall(ctx, call, requestHeaders, next);
    }
}
