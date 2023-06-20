package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.TaskState;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.Consumer;

public interface TaskService {
    void init(Consumer<TaskState> onComplete);

    void addTasks(List<TaskState> tasks);

    @Nullable
    GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId);
}
