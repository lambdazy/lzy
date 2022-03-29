package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import com.google.protobuf.ByteString;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.util.Set;
import java.util.function.Consumer;

public interface LzySlot {
    String name();
    Slot definition();

    void suspend();
    void destroy();
    void close();

    Operations.SlotStatus.State state();

    void onState(Operations.SlotStatus.State state, Runnable action);
    void onState(Set<Operations.SlotStatus.State> state, Runnable action);
    Operations.SlotStatus status();

    void onChunk(Consumer<ByteString> trafficTracker);
}
