package ru.yandex.cloud.ml.platform.lzy.server;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;

import java.net.URI;

public interface ConnectionManager {
    LzyServantBlockingStub getOrCreate(URI uri);
    void shutdownConnection(URI uri);
}
