package ru.yandex.cloud.ml.platform.lzy.server.channel;

import java.net.URI;
import java.util.UUID;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;

public interface Endpoint {
    URI uri();

    Slot slot();

    UUID sessionId();

    int connect(Endpoint endpoint);

    SlotStatus status();

    void snapshot(String snapshotId, String entryId);

    int disconnect();

    int destroy();

    void invalidate();

    boolean isInvalid();
}
