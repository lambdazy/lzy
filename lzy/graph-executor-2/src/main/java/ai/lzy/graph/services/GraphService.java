package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.DirectedGraph;
import ai.lzy.graph.model.Task;

public interface GraphService {
    DirectedGraph buildGraph(GraphExecutorApi2.GraphExecuteRequest request);
    boolean validateGraph(DirectedGraph graph);
    void createTasks(DirectedGraph graph);
    void handleTaskCompleted(Task task);
}
