package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public interface LzySlot {
    String name();
    Slot definition();

    void state(Operations.SlotStatus.State newState);

    Operations.SlotStatus status();
}
