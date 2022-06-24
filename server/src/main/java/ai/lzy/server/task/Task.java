package ai.lzy.server.task;

import java.net.URI;
import java.util.function.Consumer;
import java.util.stream.Stream;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.Zygote;
import ai.lzy.server.ServantsAllocator;
import ai.lzy.server.TasksManager;
import ai.lzy.priv.v2.Tasks;

import javax.annotation.Nullable;

public interface Task {
    String tid();
    String workloadName();

    Zygote workload();

    Stream<Slot> slots();
    Slot slot(String slotName);
    SlotStatus slotStatus(Slot slot) throws TaskException;

    State state();
    void state(State state, String... description);
    void state(State state, int rc, String... description);
    void signal(TasksManager.Signal signal) throws TaskException;

    void onProgress(Consumer<Tasks.TaskProgress> listener);

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
