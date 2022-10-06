package ai.lzy.util.grpc;

import io.grpc.*;

public class GrpcHeadersServerInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        return Contexts.interceptCall(Context.current().withValue(GrpcHeaders.HEADERS, headers), call, headers, next);
    }
}
