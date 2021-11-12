package ru.yandex.cloud.ml.platform.lzy.server;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;

import java.net.URI;
import java.util.UUID;

public interface ConnectionManager {
    LzyServantBlockingStub getOrCreate(URI uri, UUID sessionId);
    void shutdownConnection(UUID sesionId);
}
