package ru.yandex.cloud.ml.platform.lzy.server.task;

import io.grpc.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;

import java.net.URI;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface Task {
    UUID tid();

    Zygote workload();

    State state();
    void onStateChange(Consumer<State> listener);

    Slot slot(String slotName);
    SlotStatus slotStatus(Slot slot) throws TaskException;
    void signal(TasksManager.Signal signal) throws TaskException;

    URI servant();
    Channel servantChannel();
    void attachServant(URI uri);

    Stream<Slot> slots();

    void start(String token);

    enum State {
        PREPARING, CONNECTED, RUNNING, SUSPENDED, FINISHED, DESTROYED;
    }
}
