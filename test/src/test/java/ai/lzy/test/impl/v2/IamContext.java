package ai.lzy.test.impl.v2;

import ai.lzy.test.impl.IAMThreadContext;
import com.google.common.net.HostAndPort;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

@Singleton
public class IamContext {

    private final IAMThreadContext ctx;

    public IamContext() {
        ctx = new IAMThreadContext();
        ctx.init();
    }

    @PreDestroy
    public void close() {
        ctx.close();
    }

    public HostAndPort address() {
        return ctx.address();
    }

    public IAMThreadContext ctx() {
        return ctx;
    }
}
