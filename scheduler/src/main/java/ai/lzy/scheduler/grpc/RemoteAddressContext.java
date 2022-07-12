package ai.lzy.scheduler.grpc;

import io.grpc.Context;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public record RemoteAddressContext(@Nullable String remoteHost) {
    public static Context.Key<RemoteAddressContext> KEY = Context.key("remote-address");
}
