package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface ServantsAllocator {
    CompletableFuture<ServantConnection> allocate(
        String sessionId,
        Provisioning provisioning,
        Env env,
        String bucket
    );
    void shutdownSession(String sessionId);

    /** [TODO] notify task on disconnected state */
    interface ServantConnection {
        String id();
        URI uri();
        LzyServantGrpc.LzyServantBlockingStub control();
        void onProgress(Predicate<Servant.ServantProgress> tracker);
    }

    interface Ex extends ServantsAllocator {
        void register(String servantId, URI servant);
    }
}
