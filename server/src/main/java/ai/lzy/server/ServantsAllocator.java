package ai.lzy.server;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface ServantsAllocator extends SessionManager {
    CompletableFuture<ServantConnection> allocate(
        String sessionId,
        Provisioning provisioning,
        Env env
    );

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
