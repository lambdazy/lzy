package ru.yandex.cloud.ml.platform.lzy.fs;

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

    Operations.SlotStatus status();
    Operations.SlotStatus.State state();

    interface StateChangeAction extends Runnable {
        void onError(Throwable th);
    }

    default void onState(Operations.SlotStatus.State state, StateChangeAction action) {
        onState(state, action::run);
    }

    void onState(Operations.SlotStatus.State state, Runnable action);
    void onState(Set<Operations.SlotStatus.State> state, StateChangeAction action);

    void onChunk(Consumer<ByteString> trafficTracker);
}
