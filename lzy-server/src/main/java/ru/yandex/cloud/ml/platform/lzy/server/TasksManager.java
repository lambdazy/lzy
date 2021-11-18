package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface TasksManager {
    Task task(UUID tid);

    Task start(
        String uid,
        Task parent,
        Zygote workload,
        Map<Slot, String> assignments,
        boolean persistent,
        Authenticator token,
        Consumer<Servant.ExecutionProgress> progressTracker
    ) throws TaskException;

    Stream<Task> ps();
    Stream<Channel> cs();

    Channel channel(String chName);
    Channel createChannel(String name, String uid, Task parent, DataSchema contentTypeFrom);

    SlotStatus[] connected(Channel channel);

    String owner(UUID tid);

    Map<Slot, Channel> slots(String user);
    void addUserSlot(String user, Slot slot, Channel channel);
    boolean removeUserSlot(String user, Slot slot);
    void destroyUserChannels(String user);

    enum Signal {
        TOUCH(0),
        KILL(9),
        TERM(10),
        CHLD(20);

        int sig;

        public static Signal valueOf(int sigValue) {
            for (Signal value : Signal.values()) {
                if (value.sig() == sigValue)
                    return value;
            }
            return TOUCH;
        }

        public int sig() {
            return sig;
        }

        Signal(int sig) {
            this.sig = sig;
        }
    }
}
