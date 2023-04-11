package ai.lzy.util.grpc;

import io.grpc.*;

import java.net.InetSocketAddress;

public class RemoteAddressInterceptor implements ServerInterceptor {

    public static ServerInterceptor create() {
        return new RemoteAddressInterceptor();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata,
                                                                 ServerCallHandler<ReqT, RespT> serverCallHandler)
    {
        final InetSocketAddress remoteAddr = (InetSocketAddress) serverCall
                .getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        var host = remoteAddr == null ? null : remoteAddr.getHostName();
        Context context = Context.current().withValue(RemoteAddressContext.KEY, new RemoteAddressContext(host));
        return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
    }
}
