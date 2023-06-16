package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.TaskState;
import jakarta.annotation.Nullable;

import java.util.function.Consumer;

public interface TaskService {
    void addTask(TaskState task, Consumer<TaskState> onComplete);

    @Nullable
    GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId);
}
