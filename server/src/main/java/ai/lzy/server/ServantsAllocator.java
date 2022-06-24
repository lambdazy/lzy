package ai.lzy.server;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.priv.v2.LzyFsGrpc;
import ai.lzy.priv.v2.LzyServantGrpc;
import ai.lzy.priv.v2.Servant;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface ServantsAllocator extends SessionManager, AutoCloseable {
    CompletableFuture<ServantConnection> allocate(
        String sessionId,
        Provisioning provisioning,
        Env env
    );

    @Override
    default void close() {}

    /** [TODO] notify task on disconnected state */
    interface ServantConnection {
        String id();
        URI uri();
        URI fsUri();
        LzyServantGrpc.LzyServantBlockingStub control();
        LzyFsGrpc.LzyFsBlockingStub fs();
        Env env();
        Provisioning provisioning();
        void onProgress(Predicate<Servant.ServantProgress> tracker);
    }

    interface Ex extends ServantsAllocator {
        void register(String servantId, URI servant, URI fs);
    }
}
