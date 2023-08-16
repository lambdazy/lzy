package ai.lzy.graph.services;

import ai.lzy.graph.LGE;
import ai.lzy.graph.model.TaskState;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.function.Consumer;

public interface TasksScheduler {
    void start(Consumer<TaskState> onTaskFinish);

    void shutdown();

    void restoreGraphTasks(String graphId, Collection<TaskState> waiting, Collection<TaskState> running);

    void scheduleGraphTasks(String graphId, Collection<TaskState> tasks);

    void terminateGraphTasks(String graphId, String reason);

    @Nullable
    LGE.TaskExecutionStatus getTaskStatus(String taskId);
}
