package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public interface TasksManager {
    Task task(UUID tid);

    Task start(String uid, Task parent, Zygote workload, Map<String, Channel> assignments, Authenticator token) throws TaskException;

    Stream<Task> ps();
    Stream<Channel> cs();

    Channel channel(UUID chid);
    Channel createChannel(String uid, Task parent, DataSchema contentTypeFrom);
    void bind(Task task, String slotName, Channel channel) throws ChannelException;

    SlotStatus[] connected(Channel channel);

    String owner(UUID tid);

    enum Signal {
        TOUCH(0),
        KILL(9),
        TERM(10);

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
