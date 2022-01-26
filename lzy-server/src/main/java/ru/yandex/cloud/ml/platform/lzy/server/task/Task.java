package ru.yandex.cloud.ml.platform.lzy.server.task;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface Task {
    UUID tid();

    Zygote workload();

    State state();
    void onProgress(Consumer<Servant.ExecutionProgress> listener);

    Slot slot(String slotName);
    SlotStatus slotStatus(Slot slot) throws TaskException;
    void signal(TasksManager.Signal signal) throws TaskException;

    URI servant();
    void attachServant(URI uri, LzyServantBlockingStub servant);

    Stream<Slot> slots();

    String bucket();

    void start(String token);

    enum State {
        PREPARING, CONNECTED, RUNNING, SUSPENDED, FINISHED, DESTROYED;
    }

    SnapshotMeta wbMeta();
}
