package ai.lzy.graph.services;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.longrunning.Operation;

public interface GraphService {
    void runGraph(GraphExecutorApi2.GraphExecuteRequest request, Operation op) throws Exception;
}
