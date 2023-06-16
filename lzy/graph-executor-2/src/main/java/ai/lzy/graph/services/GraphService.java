package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.DirectedGraph;
import ai.lzy.graph.model.TaskState;
import jakarta.annotation.Nullable;

public interface GraphService {
    @Nullable
    DirectedGraph buildGraph(GraphExecutorApi2.GraphExecuteRequest request);

    void validateGraph(@Nullable DirectedGraph graph);

    void createTasks(DirectedGraph graph);

    void handleTaskCompleted(TaskState task);
}
