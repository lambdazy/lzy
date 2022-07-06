package ai.lzy.scheduler.grpc;

import io.grpc.Context;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

public record RemoteAddressContext(HostAndPort address) {
    public static Context.Key<RemoteAddressContext> KEY = Context.key("remote-address");
}
