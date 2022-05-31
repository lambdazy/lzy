package ru.yandex.cloud.ml.platform.lzy.server.task;

import java.net.URI;
import java.util.function.Consumer;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocator;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

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
        UNKNOWN(0), QUEUE(1), PREPARING(2), CONNECTED(3), EXECUTING(4),
        COMMUNICATION_COMPLETED(5), DISCONNECTED(6), SUCCESS(7), ERROR(8);

        private final int phaseNo;

        State(int phaseNo) {
            this.phaseNo = phaseNo;
        }

        public int phase() {
            return phaseNo;
        }
    }
}
