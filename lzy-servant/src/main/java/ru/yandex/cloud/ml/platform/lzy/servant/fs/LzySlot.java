package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public interface LzySlot {
    String name();

    Slot definition();

    void suspend();

    void destroy();

    void snapshot(String snapshotId, String entryId);

    Operations.SlotStatus.State state();

    void onState(Operations.SlotStatus.State state, Runnable action);

    Operations.SlotStatus status();

    boolean throughSnapshot();
}
