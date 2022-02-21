package ru.yandex.cloud.ml.platform.lzy.server;

import java.net.URI;
import java.util.UUID;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;

public interface ConnectionManager {
    LzyServantBlockingStub getOrCreate(URI uri, UUID sessionId);

    void shutdownConnection(UUID sessionId);
}
