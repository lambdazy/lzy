package ai.lzy.longrunning.task;

import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.List;

public interface TaskQueue {
    Task add(Task task);

    @Nullable
    Task pollNext();

    List<Task> pollRemaining();

    Task update(long id, Task.Update update);

    Task updateLease(long taskId, Duration duration);

    void delete(long id);
}
