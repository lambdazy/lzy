package ai.lzy.longrunning.task;

import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.List;

public interface TaskQueue {
    void add(Task task);

    @Nullable
    Task pollNext();

    List<Task> pollRemaining();

    void update(long id, Task.Update update);

    void updateLease(Task task, Duration duration);
}
