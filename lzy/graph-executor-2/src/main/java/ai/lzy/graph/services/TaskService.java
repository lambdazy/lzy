package ai.lzy.graph.services;

import java.util.function.Consumer;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.Task;

public interface TaskService {
    void addTask(Task task, Consumer<Task> onComplete);
    GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId);
}
