package ru.yandex.cloud.ml.platform.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;

import java.net.URI;
import java.util.UUID;

public interface Endpoint {
    URI uri();
    Slot slot();
    UUID sessionId();

    int connect(Endpoint endpoint);
    int connectPersistent(Endpoint endpoint);
    SlotStatus status();
    int disconnect();
    int destroy();

    boolean isInvalid();
}
