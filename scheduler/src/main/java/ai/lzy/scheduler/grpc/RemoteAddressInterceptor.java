package ai.lzy.scheduler.grpc;

import io.grpc.*;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import java.net.SocketAddress;

public class RemoteAddressInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata,
                                                                 ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final SocketAddress remoteAddr = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        final HostAndPort address;
        if (remoteAddr == null) {
            address = null;
        } else {
            address = HostAndPort.fromString(remoteAddr.toString());
        }
        Context context = Context.current().withValue(RemoteAddressContext.KEY, new RemoteAddressContext(address));
        return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
    }
}
