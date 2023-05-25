package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.Task;

import java.util.function.Consumer;

public interface TaskService {
    void addTask(Task task, Consumer<Task> onComplete);
    GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId);
}
