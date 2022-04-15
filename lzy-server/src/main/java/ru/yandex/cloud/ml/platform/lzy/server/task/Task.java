package ru.yandex.cloud.ml.platform.lzy.server.task;

import java.net.URI;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocator;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import javax.annotation.Nullable;

public interface Task {
    UUID tid();

    Zygote workload();

    Stream<Slot> slots();
    Slot slot(String slotName);
    SlotStatus slotStatus(Slot slot) throws TaskException;

    State state();
    void state(State state, String... description);
    void signal(TasksManager.Signal signal) throws TaskException;

    void onProgress(Consumer<Tasks.TaskProgress> listener);

    void attachServant(ServantsAllocator.ServantConnection connection);
    @Nullable
    URI servantUri();

    enum State {
        QUEUE(0), PREPARING(1), CONNECTED(2), EXECUTING(3),
        COMMUNICATION_COMPLETED(4), DISCONNECTED(5), SUCCESS(6), ERROR(7);

        private final int phaseNo;

        State(int phaseNo) {
            this.phaseNo = phaseNo;
        }

        public int phase() {
            return phaseNo;
        }
    }
}
