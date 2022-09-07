package ai.lzy.fs.fs;

import ai.lzy.model.basic.SlotInstance;
import com.google.protobuf.ByteString;
import ai.lzy.model.slot.Slot;
import ai.lzy.v1.Operations;

import java.util.Set;
import java.util.function.Consumer;

public interface LzySlot {
    default String name() {
        return definition().name();
    }
    default String taskId() {
        return instance().taskId();
    }
    Slot definition();
    SlotInstance instance();

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
