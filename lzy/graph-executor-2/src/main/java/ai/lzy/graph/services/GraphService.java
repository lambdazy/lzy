package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.model.TaskState;
import ai.lzy.longrunning.Operation;

public interface GraphService {
    void buildGraph(GraphExecutorApi2.GraphExecuteRequest request, Operation op) throws Exception;

    void handleTaskCompleted(TaskState task);
}
