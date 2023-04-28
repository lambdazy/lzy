package ai.lzy.util.grpc;

import io.grpc.Context;
import io.grpc.Metadata;
import jakarta.annotation.Nullable;

public class ProxyHeaderContext {
    public static Context.Key<ProxyHeaderContext> KEY = Context.key("header-context");

    private final Metadata headers;

    public ProxyHeaderContext(Metadata headers) {
        this.headers = headers;
    }

    @Nullable
    public static ProxyHeaderContext current() {
        return KEY.get();
    }

    public Metadata headers() {
        return headers;
    }
}
