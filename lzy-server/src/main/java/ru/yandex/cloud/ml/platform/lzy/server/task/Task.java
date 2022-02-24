package ru.yandex.cloud.ml.platform.lzy.server.task;

import java.net.URI;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public interface Task {
    UUID tid();

    Zygote workload();

    State state();

    void onProgress(Consumer<Servant.ExecutionProgress> listener);

    Slot slot(String slotName);

    SlotStatus slotStatus(Slot slot) throws TaskException;

    void signal(TasksManager.Signal signal) throws TaskException;

    URI servantUri();

    void attachServant(URI uri, LzyServantBlockingStub servant);

    void stopServant();

    boolean servantIsAlive();

    Stream<Slot> slots();

    String bucket();

    void start(String token);

    enum State {
        PREPARING, CONNECTED, RUNNING, SUSPENDED, FINISHED, DESTROYED;
    }
}
