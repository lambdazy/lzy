package ai.lzy.server;

import ai.lzy.model.slot.Slot;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.server.task.Task;
import java.util.Map;
import java.util.stream.Stream;

public interface TasksManager {
    Task task(String tid);

    Task start(String uid, Task parent, Zygote workload, Map<Slot, String> assignments, Authenticator token);

    Stream<Task> tasks();

    String owner(String tid);
}
