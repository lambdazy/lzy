package ai.lzy.scheduler.grpc;

import io.grpc.*;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class RemoteAddressInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata,
                                                                 ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final InetSocketAddress remoteAddr = (InetSocketAddress) serverCall
                .getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        var host = remoteAddr == null ? null : remoteAddr.getHostName();
        Context context = Context.current().withValue(RemoteAddressContext.KEY, new RemoteAddressContext(host));
        return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
    }
}
