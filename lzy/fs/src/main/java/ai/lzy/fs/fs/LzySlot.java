package ai.lzy.fs.fs;

import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import jakarta.annotation.Nullable;

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
    void destroy(@Nullable String error);
    void close();

    LMS.SlotStatus status();
    LMS.SlotStatus.State state();

    interface StateChangeAction extends Runnable {
        void onError(Throwable th);
    }

    default void onState(LMS.SlotStatus.State state, StateChangeAction action) {
        onState(state, action::run);
    }

    void onState(LMS.SlotStatus.State state, Runnable action);
    void onState(Set<LMS.SlotStatus.State> state, StateChangeAction action);

    void onChunk(Consumer<ByteString> trafficTracker);
}
