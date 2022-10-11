package ai.lzy.util.grpc;

import io.grpc.Context;

import javax.annotation.Nullable;

public record RemoteAddressContext(@Nullable String remoteHost) {
    public static Context.Key<RemoteAddressContext> KEY = Context.key("remote-address");
}
