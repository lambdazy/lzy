package ai.lzy.server.task;

import ai.lzy.model.Signal;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotStatus;
import ai.lzy.server.ServantsAllocator;
import ai.lzy.v1.deprecated.LzyTask;

import java.net.URI;
import java.util.function.Consumer;
import javax.annotation.Nullable;

public interface Task {
    String tid();
    String workloadName();

    Zygote workload();

    SlotStatus slotStatus(Slot slot) throws TaskException;

    State state();
    void state(State state, String... description);
    void state(State state, int rc, String... description);
    void signal(Signal signal) throws TaskException;

    void onProgress(Consumer<LzyTask.TaskProgress> listener);

    void attachServant(ServantsAllocator.ServantConnection connection);
    @Nullable
    URI servantUri();
    @Nullable
    URI servantFsUri();

    enum State {
        UNKNOWN(0), QUEUE(1), PREPARING(2), EXECUTING(4), SUCCESS(5),
        ERROR(6);

        private final int phaseNo;

        State(int phaseNo) {
            this.phaseNo = phaseNo;
        }

        public int phase() {
            return phaseNo;
        }
    }
}
