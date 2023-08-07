package ai.lzy.graph.services;

import ai.lzy.graph.LGE;
import ai.lzy.graph.model.TaskState;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.Consumer;

public interface TaskService {
    void init(Consumer<TaskState> onComplete);

    void addTasks(List<TaskState> tasks);

    @Nullable
    LGE.TaskExecutionStatus getTaskStatus(String taskId);
}
