package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.channel.ChannelSpec;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public interface TasksManager {
    Task task(UUID tid);

    Task start(
        String uid,
        Task parent,
        Zygote workload,
        Map<Slot, String> assignments,
        Authenticator token,
        Consumer<Servant.ExecutionProgress> progressTracker,
        String bucket
    ) throws TaskException;

    Stream<Task> ps();

    Stream<ChannelSpec> cs();

    ChannelSpec channel(String chName);

    ChannelSpec createChannel(String uid, Task parent, ChannelSpec channelSpec);

    SlotStatus[] connected(ChannelSpec channel);

    String owner(UUID tid);

    Map<Slot, ChannelSpec> slots(String user);

    void addUserSlot(String user, Slot slot, ChannelSpec channel);

    boolean removeUserSlot(String user, Slot slot);

    void destroyUserChannels(String user);

    enum Signal {
        TOUCH(0),
        KILL(9),
        TERM(10),
        CHLD(20);

        int sig;

        Signal(int sig) {
            this.sig = sig;
        }

        public static Signal valueOf(int sigValue) {
            for (Signal value : Signal.values()) {
                if (value.sig() == sigValue) {
                    return value;
                }
            }
            return TOUCH;
        }

        public int sig() {
            return sig;
        }
    }
}
